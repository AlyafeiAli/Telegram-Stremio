import asyncio
import traceback
import PTN
import re
from re import compile, IGNORECASE
from Backend.helper.imdb import get_detail, get_season, search_title
from themoviedb import aioTMDb
from Backend.config import Telegram
import Backend
from Backend.logger import LOGGER
from Backend.helper.encrypt import encode_string

# ----------------- Configuration -----------------
DELAY = 0
tmdb = aioTMDb(key=Telegram.TMDB_API, language="en-US", region="US")

# Cache dictionaries (per run)
IMDB_CACHE: dict = {}
TMDB_SEARCH_CACHE: dict = {}
TMDB_DETAILS_CACHE: dict = {}
EPISODE_CACHE: dict = {}

# Concurrency semaphore for external API calls
API_SEMAPHORE = asyncio.Semaphore(12)

# ----------------- Patterns -----------------
CRC32_PATTERN = compile(r'\s*[\(\[]\s*[0-9A-Fa-f]{8}\s*[\)\]]\s*', IGNORECASE)
MULTIPART_PATTERN = compile(r'(?:part|cd|disc|disk)[s._-]*\d+(?=\.\w+$)', IGNORECASE)
BRACKET_GROUP_PATTERN = compile(r'^\[[^\]]+\]\s*')  # leading fansub group tag e.g. [Kaiyou]

# Matches a standalone bracketed 1-4 digit number that isn't a resolution.
# e.g. [03], [12], [001], but not [1080p] (already has 'p' after digits)
ANIME_EP_BRACKET_PATTERN = compile(
    r'\[\s*(\d{1,4})\s*\](?!\s*p)',
    IGNORECASE
)

# ----------------- Helpers -----------------
def strip_crc32(filename: str) -> str:
    """Remove CRC32 checksums like [A1B2C3D4] or (a1b2c3d4) from filename."""
    return CRC32_PATTERN.sub('', filename).strip()


def pre_clean_anime_filename(filename: str) -> str:
    """
    Prepare filename for PTN parsing:
      1. Take basename only
      2. Strip CRC32 checksum
      3. Strip leading fansub group tag e.g. [Kaiyou]
    This ensures PTN sees bracketed episode numbers like [03] correctly.
    """
    name = filename.rsplit("/", 1)[-1]             # basename
    name = strip_crc32(name)                        # remove [DEADBEEF]
    name = BRACKET_GROUP_PATTERN.sub('', name)      # remove [GroupName] prefix
    return name


def extract_fallback_resolution(filename: str) -> str | None:
    """
    Catch non-standard resolution strings PTN doesn't recognize,
    e.g. Ma10p_1080p, Hi10_720p. Returns normalized value like '1080p'.
    """
    match = re.search(r'(\d{3,4}p)', filename, IGNORECASE)
    return match.group(1) if match else None


def extract_anime_episode(filename: str) -> int | None:
    """
    Extract anime episode from [NN] style brackets when PTN misses it.
    Skips bracketed numbers that look like years (1900-2099).
    Returns the first plausible episode number found.
    """
    for match in ANIME_EP_BRACKET_PATTERN.finditer(filename):
        num = int(match.group(1))
        # Skip likely years
        if 1900 <= num <= 2099:
            continue
        # Reasonable episode range
        if 1 <= num <= 1999:
            return num
    return None


def clean_anime_title(title: str) -> str:
    """
    Strip leftover episode brackets that PTN may have glued into the title
    when it didn't recognize [NN] as an episode marker.
    """
    if not title:
        return title
    cleaned = re.sub(r'\s*\[\s*\d{1,4}\s*\]\s*', ' ', title)
    return cleaned.strip()


def format_tmdb_image(path: str, size="w500") -> str:
    if not path:
        return ""
    return f"https://image.tmdb.org/t/p/{size}{path}"


def get_tmdb_logo(images) -> str:
    if not images:
        return ""
    logos = getattr(images, "logos", None)
    if not logos:
        return ""
    for logo in logos:
        iso_lang = getattr(logo, "iso_639_1", None)
        file_path = getattr(logo, "file_path", None)
        if iso_lang == "en" and file_path:
            return format_tmdb_image(file_path, "w300")
    for logo in logos:
        file_path = getattr(logo, "file_path", None)
        if file_path:
            return format_tmdb_image(file_path, "w300")
    return ""


def format_imdb_images(imdb_id: str) -> dict:
    if not imdb_id:
        return {"poster": "", "backdrop": "", "logo": ""}
    return {
        "poster": f"https://images.metahub.space/poster/small/{imdb_id}/img",
        "backdrop": f"https://images.metahub.space/background/medium/{imdb_id}/img",
        "logo": f"https://images.metahub.space/logo/medium/{imdb_id}/img",
    }


def extract_default_id(url: str) -> str | None:
    imdb_match = re.search(r'/title/(tt\d+)', url)
    if imdb_match:
        return imdb_match.group(1)
    tmdb_match = re.search(r'/((movie|tv))/(\d+)', url)
    if tmdb_match:
        return tmdb_match.group(3)
    return None


async def safe_imdb_search(title: str, type_: str) -> str | None:
    key = f"imdb::{type_}::{title}"
    if key in IMDB_CACHE:
        return IMDB_CACHE[key]
    try:
        async with API_SEMAPHORE:
            result = await search_title(query=title, type=type_)
        imdb_id = result["id"] if result else None
        IMDB_CACHE[key] = imdb_id
        return imdb_id
    except Exception as e:
        LOGGER.warning(f"IMDb search failed for '{title}' [{type_}]: {e}")
        return None


async def safe_tmdb_search(title: str, type_: str, year=None):
    key = f"tmdb_search::{type_}::{title}::{year}"
    if key in TMDB_SEARCH_CACHE:
        return TMDB_SEARCH_CACHE[key]
    try:
        async with API_SEMAPHORE:
            if type_ == "movie":
                results = await tmdb.search().movies(query=title, year=year) if year else await tmdb.search().movies(query=title)
            else:
                results = await tmdb.search().tv(query=title)
        res = results[0] if results else None
        TMDB_SEARCH_CACHE[key] = res
        return res
    except Exception as e:
        LOGGER.error(f"TMDb search failed for '{title}' [{type_}]: {e}")
        TMDB_SEARCH_CACHE[key] = None
        return None


async def _tmdb_movie_details(movie_id):
    if movie_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[movie_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.movie(movie_id).details(
                append_to_response="external_ids,credits"
            )
            images = await tmdb.movie(movie_id).images()
            details.images = images
        TMDB_DETAILS_CACHE[movie_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb movie details fetch failed for id={movie_id}: {e}")
        TMDB_DETAILS_CACHE[movie_id] = None
        return None


async def _tmdb_tv_details(tv_id):
    if tv_id in TMDB_DETAILS_CACHE:
        return TMDB_DETAILS_CACHE[tv_id]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.tv(tv_id).details(
                append_to_response="external_ids,credits"
            )
            images = await tmdb.tv(tv_id).images()
            details.images = images
        TMDB_DETAILS_CACHE[tv_id] = details
        return details
    except Exception as e:
        LOGGER.warning(f"TMDb tv details fetch failed for id={tv_id}: {e}")
        TMDB_DETAILS_CACHE[tv_id] = None
        return None


async def _tmdb_episode_details(tv_id, season, episode):
    key = (tv_id, season, episode)
    if key in EPISODE_CACHE:
        return EPISODE_CACHE[key]
    try:
        async with API_SEMAPHORE:
            details = await tmdb.episode(tv_id, season, episode).details()
        EPISODE_CACHE[key] = details
        return details
    except Exception:
        EPISODE_CACHE[key] = None
        return None


async def resolve_anime_absolute_episode(tmdb_id: int, absolute: int) -> tuple[int, int] | None:
    """
    Walk TMDb seasons sequentially (skipping specials/season 0), counting
    episodes, until we locate which season+episode the absolute number falls in.
    """
    cache_key = f"anime_abs::{tmdb_id}::{absolute}"
    if cache_key in EPISODE_CACHE:
        return EPISODE_CACHE[cache_key]

    try:
        tv = await _tmdb_tv_details(tmdb_id)
        if not tv:
            return None

        seasons = [s for s in (tv.seasons or []) if getattr(s, "season_number", 0) > 0]
        seasons.sort(key=lambda s: s.season_number)

        counter = 0
        for season_obj in seasons:
            season_num = season_obj.season_number
            ep_count = getattr(season_obj, "episode_count", 0)

            if counter + ep_count >= absolute:
                ep_num = absolute - counter
                result = (season_num, ep_num)
                EPISODE_CACHE[cache_key] = result
                return result

            counter += ep_count

    except Exception as e:
        LOGGER.warning(f"Anime absolute resolution failed for tmdb_id={tmdb_id}, abs={absolute}: {e}")

    EPISODE_CACHE[cache_key] = None
    return None


# ----------------- Main Metadata -----------------
async def metadata(filename: str, channel: int, msg_id, override_id: str = None) -> dict | None:
    clean_filename = pre_clean_anime_filename(filename)

    try:
        parsed = PTN.parse(clean_filename)
    except Exception as e:
        LOGGER.error(f"PTN parsing failed for {filename}: {e}\n{traceback.format_exc()}")
        return None

    # Skip combined/invalid files
    if "excess" in parsed and any("combined" in item.lower() for item in parsed["excess"]):
        LOGGER.info(f"Skipping {filename}: contains 'combined'")
        return None

    # Skip split/multipart files
    if MULTIPART_PATTERN.search(clean_filename):
        LOGGER.info(f"Skipping {filename}: seems to be a split/multipart file")
        return None

    title = parsed.get("title")
    season = parsed.get("season")
    episode = parsed.get("episode")
    year = parsed.get("year")
    quality = parsed.get("resolution") or extract_fallback_resolution(filename)

    # ----------------- Anime [NN] episode fallback -----------------
    # PTN doesn't recognize bare bracketed numbers like [03] as episodes
    # when there's no season indicator (S01, 1x03, etc.). For anime releases
    # we need to detect these manually so we route to TV metadata instead
    # of falling through to the movie branch.
    if not episode and not season:
        fallback_ep = extract_anime_episode(clean_filename)
        if fallback_ep:
            episode = fallback_ep
            # Clean any episode bracket that PTN may have glued into the title
            title = clean_anime_title(title)
            LOGGER.info(f"Anime [NN] fallback: '{title}' episode #{episode}")

    if isinstance(season, list) or isinstance(episode, list):
        LOGGER.warning(f"Invalid season/episode format for {filename}: {parsed}")
        return None
    if season and not episode:
        LOGGER.warning(f"Missing episode in {filename}: {parsed}")
        return None
    if not quality:
        LOGGER.warning(f"Skipping {filename}: No resolution (parsed={parsed})")
        return None
    if not title:
        LOGGER.info(f"No title parsed from: {filename} (parsed={parsed})")
        return None

    default_id = None
    if override_id:
        try:
            default_id = extract_default_id(override_id) or override_id
        except Exception:
            pass

    if not default_id:
        try:
            default_id = extract_default_id(Backend.USE_DEFAULT_ID)
        except Exception:
            pass

    if not default_id:
        try:
            default_id = extract_default_id(clean_filename)
        except Exception:
            pass

    data = {"chat_id": channel, "msg_id": msg_id}
    try:
        encoded_string = await encode_string(data)
    except Exception:
        encoded_string = None

    try:
        if season and episode:
            LOGGER.info(f"Fetching TV metadata: {title} S{season}E{episode}")
            return await fetch_tv_metadata(title, season, episode, encoded_string, year, quality, default_id)
        elif not season and episode:
            # No season — treat as anime with absolute episode numbering
            LOGGER.info(f"No season detected for {filename}, treating as anime absolute episode")
            return await fetch_tv_metadata(title, season, episode, encoded_string, year, quality, default_id)
        else:
            LOGGER.info(f"Fetching Movie metadata: {title} ({year})")
            return await fetch_movie_metadata(title, encoded_string, year, quality, default_id)
    except Exception as e:
        LOGGER.error(f"Error while fetching metadata for {filename}: {e}\n{traceback.format_exc()}")
        return None


# ----------------- TV Metadata -----------------
async def fetch_tv_metadata(title, season, episode, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    imdb_tv = None
    imdb_ep = None
    use_tmdb = False

    # -------------------------------------------------------
    # 1. Handle default ID (IMDb / TMDb)
    # -------------------------------------------------------
    if default_id:
        default_id = str(default_id)
        if default_id.startswith("tt"):
            imdb_id = default_id
            use_tmdb = False
        elif default_id.isdigit():
            tmdb_id = int(default_id)
            use_tmdb = True

    # -------------------------------------------------------
    # 2. If no ID → Try IMDb search first
    # -------------------------------------------------------
    if not imdb_id and not tmdb_id:
        imdb_id = await safe_imdb_search(title, "tvSeries")
        use_tmdb = not bool(imdb_id)

    # -------------------------------------------------------
    # 3. Anime absolute episode resolution
    #    Triggered when: no season, episode present
    # -------------------------------------------------------
    absolute_episode = None
    if not season and episode:
        absolute_episode = int(episode)
        LOGGER.info(f"Detected anime absolute episode: {title} #{absolute_episode}")

        if not tmdb_id:
            tmdb_search = await safe_tmdb_search(title, "tv", year)
            if not tmdb_search:
                LOGGER.warning(f"No TMDb result for anime '{title}', cannot resolve absolute episode")
                return None
            tmdb_id = tmdb_search.id
            use_tmdb = True

        resolved = await resolve_anime_absolute_episode(tmdb_id, absolute_episode)
        if resolved:
            season, episode = resolved
            LOGGER.info(f"Resolved abs#{absolute_episode} → S{season}E{episode} for '{title}'")
        else:
            season, episode = 1, absolute_episode
            LOGGER.warning(
                f"Could not resolve abs#{absolute_episode} for '{title}', "
                f"falling back to S1E{absolute_episode}"
            )

    # -------------------------------------------------------
    # 4. IMDb fetch (series + episode)
    # -------------------------------------------------------
    if imdb_id and not use_tmdb:
        try:
            if imdb_id in IMDB_CACHE:
                imdb_tv = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
                IMDB_CACHE[imdb_id] = imdb_tv

            ep_key = f"{imdb_id}::{season}::{episode}"
            if ep_key in EPISODE_CACHE:
                imdb_ep = EPISODE_CACHE[ep_key]
            else:
                async with API_SEMAPHORE:
                    imdb_ep = await get_season(imdb_id=imdb_id, season_id=season, episode_id=episode)
                EPISODE_CACHE[ep_key] = imdb_ep

        except Exception as e:
            LOGGER.warning(f"IMDb TV fetch failed [{imdb_id}] → {e}")
            imdb_tv = None
            imdb_ep = None
            use_tmdb = True

    # -------------------------------------------------------
    # 5. Decide if TMDb is required
    # -------------------------------------------------------
    must_use_tmdb = (
        use_tmdb or
        imdb_tv is None or
        imdb_tv == {}
    )

    # =======================================================
    #  6. TMDb MODE
    # =======================================================
    if must_use_tmdb:
        LOGGER.info(f"No valid IMDb TV data for '{title}' → using TMDb")

        if not tmdb_id:
            tmdb_search = await safe_tmdb_search(title, "tv", year)
            if not tmdb_search:
                LOGGER.warning(f"No TMDb TV result for '{title}'")
                return None
            tmdb_id = tmdb_search.id

        tv = await _tmdb_tv_details(tmdb_id)
        if not tv:
            LOGGER.warning(f"TMDb TV details failed for id={tmdb_id}")
            return None

        ep = await _tmdb_episode_details(tmdb_id, season, episode)

        credits = getattr(tv, "credits", None) or {}
        cast_arr = getattr(credits, "cast", []) or []
        cast = [
            getattr(c, "name", None) or getattr(c, "original_name", None)
            for c in cast_arr
        ]

        ep_runtime = getattr(ep, "runtime", None) if ep else None
        series_runtime = (
            tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
        )
        runtime_val = ep_runtime or series_runtime
        runtime = f"{runtime_val} min" if runtime_val else ""

        return {
            "tmdb_id": tv.id,
            "imdb_id": getattr(getattr(tv, "external_ids", None), "imdb_id", None),
            "title": tv.name,
            "year": getattr(tv.first_air_date, "year", 0) if getattr(tv, "first_air_date", None) else 0,
            "rate": getattr(tv, "vote_average", 0) or 0,
            "description": tv.overview or "",
            "poster": format_tmdb_image(tv.poster_path),
            "backdrop": format_tmdb_image(tv.backdrop_path, "original"),
            "logo": get_tmdb_logo(getattr(tv, "images", None)),
            "genres": [g.name for g in (tv.genres or [])],
            "media_type": "tv",
            "cast": cast,
            "runtime": str(runtime),

            "season_number": season,
            "episode_number": episode,
            "absolute_episode": absolute_episode,
            "episode_title": getattr(ep, "name", f"S{season}E{episode}") if ep else f"S{season}E{episode}",
            "episode_backdrop": format_tmdb_image(getattr(ep, "still_path", None), "original") if ep else "",
            "episode_overview": getattr(ep, "overview", "") if ep else "",
            "episode_released": (
                ep.air_date.strftime("%Y-%m-%dT05:00:00.000Z")
                if getattr(ep, "air_date", None)
                else ""
            ),

            "quality": quality,
            "encoded_string": encoded_string,
        }

    # =======================================================
    #  7. IMDb MODE
    # =======================================================
    imdb = imdb_tv or {}
    ep = imdb_ep or {}
    images = format_imdb_images(imdb_id)

    return {
        "tmdb_id": imdb.get("moviedb_id") or imdb_id.replace("tt", ""),
        "imdb_id": imdb_id,
        "title": imdb.get("title", title),
        "year": imdb.get("releaseDetailed", {}).get("year", 0),
        "rate": imdb.get("rating", {}).get("star", 0),
        "description": imdb.get("plot", ""),
        "poster": images["poster"],
        "backdrop": images["backdrop"],
        "logo": images["logo"],
        "cast": imdb.get("cast", []),
        "runtime": str(imdb.get("runtime") or ""),
        "genres": imdb.get("genre", []),
        "media_type": "tv",

        "season_number": season,
        "episode_number": episode,
        "absolute_episode": absolute_episode,
        "episode_title": ep.get("title", f"S{season}E{episode}"),
        "episode_backdrop": ep.get("image", ""),
        "episode_overview": ep.get("plot", ""),
        "episode_released": str(ep.get("released", "")),

        "quality": quality,
        "encoded_string": encoded_string,
    }


# ----------------- Movie Metadata -----------------
async def fetch_movie_metadata(title, encoded_string, year=None, quality=None, default_id=None) -> dict | None:
    imdb_id = None
    tmdb_id = None
    imdb_details = None
    use_tmdb = False

    # -------------------------------------------------------
    # 1. Process default ID (tt = IMDb, digits = TMDb)
    # -------------------------------------------------------
    if default_id:
        default_id = str(default_id).strip()
        if default_id.startswith("tt"):
            imdb_id = default_id
            use_tmdb = False
        elif default_id.isdigit():
            tmdb_id = int(default_id)
            use_tmdb = True

    # -------------------------------------------------------
    # 2. If no default ID → search IMDb first
    # -------------------------------------------------------
    if not imdb_id and not tmdb_id:
        imdb_id = await safe_imdb_search(
            f"{title} {year}" if year else title,
            "movie"
        )
        use_tmdb = not bool(imdb_id)

    # -------------------------------------------------------
    # 3. Fetch IMDb details
    # -------------------------------------------------------
    if imdb_id and not use_tmdb:
        try:
            if imdb_id in IMDB_CACHE:
                imdb_details = IMDB_CACHE[imdb_id]
            else:
                async with API_SEMAPHORE:
                    imdb_details = await get_detail(imdb_id=imdb_id, media_type="movie")
                IMDB_CACHE[imdb_id] = imdb_details
        except Exception as e:
            LOGGER.warning(f"IMDb movie fetch failed [{title}] → {e}")
            imdb_details = None
            use_tmdb = True

    # -------------------------------------------------------
    # 4. Decide final data source
    # -------------------------------------------------------
    must_use_tmdb = (
        use_tmdb or
        imdb_details is None or
        imdb_details == {}
    )

    # =======================================================
    #  5. TMDb MODE
    # =======================================================
    if must_use_tmdb:
        LOGGER.info(f"No valid IMDb data for '{title}' → using TMDb")

        if not tmdb_id:
            tmdb_result = await safe_tmdb_search(title, "movie", year)
            if not tmdb_result:
                LOGGER.warning(f"No TMDb movie found for '{title}'")
                return None
            tmdb_id = tmdb_result.id

        movie = await _tmdb_movie_details(tmdb_id)
        if not movie:
            LOGGER.warning(f"TMDb details failed for {tmdb_id}")
            return None

        credits = getattr(movie, "credits", None) or {}
        cast_arr = getattr(credits, "cast", []) or []
        cast_names = [
            getattr(c, "name", None) or getattr(c, "original_name", None)
            for c in cast_arr
        ]

        runtime_val = getattr(movie, "runtime", None)
        runtime = f"{runtime_val} min" if runtime_val else ""

        return {
            "tmdb_id": movie.id,
            "imdb_id": getattr(movie.external_ids, "imdb_id", None),
            "title": movie.title,
            "year": getattr(movie.release_date, "year", 0) if getattr(movie, "release_date", None) else 0,
            "rate": getattr(movie, "vote_average", 0) or 0,
            "description": movie.overview or "",
            "poster": format_tmdb_image(movie.poster_path),
            "backdrop": format_tmdb_image(movie.backdrop_path, "original"),
            "logo": get_tmdb_logo(getattr(movie, "images", None)),
            "cast": cast_names,
            "runtime": str(runtime),
            "media_type": "movie",
            "genres": [g.name for g in (movie.genres or [])],
            "quality": quality,
            "encoded_string": encoded_string,
        }

    # =======================================================
    #  6. IMDb MODE
    # =======================================================
    images = format_imdb_images(imdb_id)
    imdb = imdb_details or {}

    return {
        "tmdb_id": imdb.get("moviedb_id") or imdb_id.replace("tt", ""),
        "imdb_id": imdb_id,
        "title": imdb.get("title", title),
        "year": imdb.get("releaseDetailed", {}).get("year", 0),
        "rate": imdb.get("rating", {}).get("star", 0),
        "description": imdb.get("plot", ""),
        "poster": images["poster"],
        "backdrop": images["backdrop"],
        "logo": images["logo"],
        "cast": imdb.get("cast", []),
        "runtime": str(imdb.get("runtime") or ""),
        "media_type": "movie",
        "genres": imdb.get("genre", []),
        "quality": quality,
        "encoded_string": encoded_string,
    }


# ----------------- Candidate Search -----------------
async def search_movie_candidates(query: str, year: int | None = None, limit: int = 8) -> list[dict]:
    query = (query or "").strip()
    if not query:
        return []

    results: list[dict] = []
    seen: set[tuple[str, str]] = set()

    try:
        imdb_result = await search_title(query=query, type="movie")
        if imdb_result and imdb_result.get("id"):
            key = ("imdb", imdb_result["id"])
            if key not in seen:
                seen.add(key)
                results.append({
                    "source": "imdb",
                    "title": imdb_result.get("title", ""),
                    "year": imdb_result.get("year", ""),
                    "imdb_id": imdb_result.get("id"),
                    "tmdb_id": imdb_result.get("moviedb_id"),
                    "poster": imdb_result.get("poster", ""),
                    "backdrop": "",
                    "subtitle": "IMDb / Cinemeta",
                })
    except Exception as e:
        LOGGER.warning(f"IMDb movie candidate search failed for '{query}': {e}")

    try:
        async with API_SEMAPHORE:
            tmdb_results = await tmdb.search().movies(query=query, year=year) if year else await tmdb.search().movies(query=query)

        for item in (tmdb_results or [])[:limit]:
            tmdb_id = getattr(item, "id", None)
            if not tmdb_id:
                continue

            imdb_id = None
            try:
                details = await _tmdb_movie_details(tmdb_id)
                ext = getattr(details, "external_ids", None) if details else None
                imdb_id = getattr(ext, "imdb_id", None) if ext else None
            except Exception:
                pass

            key = ("tmdb", str(tmdb_id))
            if key in seen:
                continue
            seen.add(key)

            release_date = getattr(item, "release_date", None)
            year_value = getattr(release_date, "year", None) if release_date else None

            results.append({
                "source": "tmdb",
                "title": getattr(item, "title", "") or "",
                "year": year_value or "",
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "poster": format_tmdb_image(getattr(item, "poster_path", None)),
                "backdrop": format_tmdb_image(getattr(item, "backdrop_path", None), "original"),
                "subtitle": "TMDb",
            })
    except Exception as e:
        LOGGER.warning(f"TMDb movie candidate search failed for '{query}': {e}")

    return results[:limit]


async def search_tv_candidates(query: str, limit: int = 8) -> list[dict]:
    query = (query or "").strip()
    if not query:
        return []

    results: list[dict] = []
    seen: set[tuple[str, str]] = set()

    try:
        imdb_result = await search_title(query=query, type="tvSeries")
        if imdb_result and imdb_result.get("id"):
            key = ("imdb", imdb_result["id"])
            if key not in seen:
                seen.add(key)
                results.append({
                    "source": "imdb",
                    "title": imdb_result.get("title", ""),
                    "year": imdb_result.get("year", ""),
                    "imdb_id": imdb_result.get("id"),
                    "tmdb_id": imdb_result.get("moviedb_id"),
                    "poster": imdb_result.get("poster", ""),
                    "backdrop": "",
                    "subtitle": "IMDb / Cinemeta",
                })
    except Exception as e:
        LOGGER.warning(f"IMDb TV candidate search failed for '{query}': {e}")

    try:
        async with API_SEMAPHORE:
            tmdb_results = await tmdb.search().tv(query=query)

        for item in (tmdb_results or [])[:limit]:
            tmdb_id = getattr(item, "id", None)
            if not tmdb_id:
                continue

            imdb_id = None
            try:
                details = await _tmdb_tv_details(tmdb_id)
                ext = getattr(details, "external_ids", None) if details else None
                imdb_id = getattr(ext, "imdb_id", None) if ext else None
            except Exception:
                pass

            key = ("tmdb", str(tmdb_id))
            if key in seen:
                continue
            seen.add(key)

            first_air_date = getattr(item, "first_air_date", None)
            year_value = getattr(first_air_date, "year", None) if first_air_date else None

            results.append({
                "source": "tmdb",
                "title": getattr(item, "name", "") or "",
                "year": year_value or "",
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "poster": format_tmdb_image(getattr(item, "poster_path", None)),
                "backdrop": format_tmdb_image(getattr(item, "backdrop_path", None), "original"),
                "subtitle": "TMDb",
            })
    except Exception as e:
        LOGGER.warning(f"TMDb TV candidate search failed for '{query}': {e}")

    return results[:limit]


async def fetch_selected_movie_metadata(selected_id: str) -> dict | None:
    selected_id = str(selected_id).strip()
    if not selected_id:
        return None

    data = await fetch_movie_metadata(
        title="manual-rescan",
        encoded_string=None,
        year=None,
        quality=None,
        default_id=selected_id
    )
    if not data:
        return None

    return {
        "tmdb_id": data.get("tmdb_id"),
        "imdb_id": data.get("imdb_id"),
        "title": data.get("title"),
        "release_year": data.get("year"),
        "rating": data.get("rate"),
        "description": data.get("description"),
        "poster": data.get("poster"),
        "backdrop": data.get("backdrop"),
        "logo": data.get("logo"),
        "genres": data.get("genres", []),
        "cast": data.get("cast", []),
        "runtime": data.get("runtime"),
        "media_type": "movie",
    }


async def fetch_selected_tv_metadata(selected_id: str) -> dict | None:
    selected_id = str(selected_id).strip()
    if not selected_id:
        return None

    imdb_id = None
    tmdb_id = None
    imdb_tv = None
    use_tmdb = False

    if selected_id.startswith("tt"):
        imdb_id = selected_id
    elif selected_id.isdigit():
        tmdb_id = int(selected_id)
        use_tmdb = True
    else:
        return None

    if imdb_id and not use_tmdb:
        try:
            imdb_tv = await get_detail(imdb_id=imdb_id, media_type="tvSeries")
        except Exception:
            imdb_tv = None
            use_tmdb = True

    if use_tmdb or not imdb_tv:
        if not tmdb_id and imdb_tv and imdb_tv.get("moviedb_id"):
            try:
                tmdb_id = int(imdb_tv["moviedb_id"])
            except Exception:
                tmdb_id = None

        if not tmdb_id:
            return None

        tv = await _tmdb_tv_details(tmdb_id)
        if not tv:
            return None

        credits = getattr(tv, "credits", None) or {}
        cast_arr = getattr(credits, "cast", []) or []
        cast = [
            getattr(c, "name", None) or getattr(c, "original_name", None)
            for c in cast_arr
        ]

        runtime_val = tv.episode_run_time[0] if getattr(tv, "episode_run_time", None) else None
        runtime = f"{runtime_val} min" if runtime_val else ""

        return {
            "tmdb_id": tv.id,
            "imdb_id": getattr(getattr(tv, "external_ids", None), "imdb_id", None),
            "title": tv.name,
            "release_year": getattr(tv.first_air_date, "year", 0) if getattr(tv, "first_air_date", None) else 0,
            "rating": getattr(tv, "vote_average", 0) or 0,
            "description": tv.overview or "",
            "poster": format_tmdb_image(tv.poster_path),
            "backdrop": format_tmdb_image(tv.backdrop_path, "original"),
            "logo": get_tmdb_logo(getattr(tv, "images", None)),
            "genres": [g.name for g in (tv.genres or [])],
            "cast": cast,
            "runtime": str(runtime),
            "media_type": "tv",
        }

    images = format_imdb_images(imdb_id)
    return {
        "tmdb_id": int(imdb_tv.get("moviedb_id")) if imdb_tv.get("moviedb_id") else None,
        "imdb_id": imdb_id,
        "title": imdb_tv.get("title", ""),
        "release_year": imdb_tv.get("releaseDetailed", {}).get("year", 0),
        "rating": imdb_tv.get("rating", {}).get("star", 0),
        "description": imdb_tv.get("plot", ""),
        "poster": images["poster"],
        "backdrop": images["backdrop"],
        "logo": images["logo"],
        "genres": imdb_tv.get("genre", []),
        "cast": imdb_tv.get("cast", []),
        "runtime": str(imdb_tv.get("runtime") or ""),
        "media_type": "tv",
    }
