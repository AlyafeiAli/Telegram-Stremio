# """
# Kitsu API helper — async client for the public Kitsu JSON:API.

# Kitsu is an anime/manga discovery platform whose API natively supports
# absolute episode numbering — exactly what anime release naming (e.g.
# [Kaiyou] slamdunk [03]) gives us. Most public GET endpoints require no
# auth.

# Base URL: https://kitsu.io/api/edge
# Docs:     https://hummingbird-me.github.io/api-docs/

# This module returns plain dicts (the JSON:API `attributes` payload with
# some normalisation), not heavyweight model classes, to keep integration
# with the existing metadata pipeline simple.
# """

# import asyncio
# import aiohttp
# from typing import Optional

# from Backend.logger import LOGGER


# KITSU_BASE = "https://kitsu.io/api/edge"
# KITSU_HEADERS = {
#     "Accept": "application/vnd.api+json",
#     "Content-Type": "application/vnd.api+json",
# }
# KITSU_TIMEOUT = aiohttp.ClientTimeout(total=20)


# # ----------------- Low-level fetch -----------------
# async def _kitsu_get(path: str, params: dict | None = None) -> dict | None:
#     """GET a Kitsu endpoint and return parsed JSON, or None on failure."""
#     url = f"{KITSU_BASE}/{path.lstrip('/')}"
#     try:
#         async with aiohttp.ClientSession(
#             headers=KITSU_HEADERS, timeout=KITSU_TIMEOUT
#         ) as session:
#             async with session.get(url, params=params) as resp:
#                 if resp.status != 200:
#                     LOGGER.warning(
#                         f"Kitsu GET {url} returned status {resp.status}"
#                     )
#                     return None
#                 return await resp.json()
#     except asyncio.TimeoutError:
#         LOGGER.warning(f"Kitsu GET {url} timed out")
#         return None
#     except Exception as e:
#         LOGGER.warning(f"Kitsu GET {url} failed: {e}")
#         return None


# # ----------------- Title normalisation -----------------
# def _pick_title(titles: dict | None, canonical: str | None) -> str:
#     """
#     Choose the best display title from Kitsu's titles dict.
#     Priority: English → romanised Japanese → canonical → first available.
#     """
#     titles = titles or {}
#     for key in ("en", "en_us", "en_jp", "ja_jp"):
#         val = titles.get(key)
#         if val:
#             return val
#     if canonical:
#         return canonical
#     for val in titles.values():
#         if val:
#             return val
#     return ""


# def _poster_url(images: dict | None, size: str = "large") -> str:
#     if not images:
#         return ""
#     return images.get(size) or images.get("original") or images.get("medium") or ""


# # ----------------- Public API -----------------
# async def search_anime(title: str, limit: int = 5) -> list[dict]:
#     """
#     Search Kitsu by title. Returns a list of simplified anime dicts.
#     Uses filter[text] which performs a fuzzy match across known titles.
#     """
#     if not title:
#         return []

#     data = await _kitsu_get(
#         "anime",
#         params={
#             "filter[text]": title,
#             "page[limit]": str(limit),
#             "fields[anime]": (
#                 "slug,titles,canonicalTitle,synopsis,startDate,endDate,"
#                 "episodeCount,subtype,posterImage,coverImage,averageRating,"
#                 "ageRating,status,categories"
#             ),
#         },
#     )
#     if not data or not data.get("data"):
#         return []

#     results = []
#     for item in data["data"]:
#         attrs = item.get("attributes", {}) or {}
#         results.append({
#             "kitsu_id": item.get("id"),
#             "slug": attrs.get("slug"),
#             "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
#             "canonical_title": attrs.get("canonicalTitle", ""),
#             "titles": attrs.get("titles", {}) or {},
#             "synopsis": attrs.get("synopsis", "") or "",
#             "start_date": attrs.get("startDate", "") or "",
#             "end_date": attrs.get("endDate", "") or "",
#             "episode_count": attrs.get("episodeCount"),
#             "subtype": attrs.get("subtype", "") or "",          # TV, movie, OVA, ONA, special
#             "status": attrs.get("status", "") or "",
#             "poster": _poster_url(attrs.get("posterImage")),
#             "backdrop": _poster_url(attrs.get("coverImage"), "large"),
#             "rating": attrs.get("averageRating"),
#         })
#     return results


# async def get_anime(kitsu_id: str | int) -> dict | None:
#     """Fetch a single anime's full details by Kitsu ID."""
#     if not kitsu_id:
#         return None

#     data = await _kitsu_get(f"anime/{kitsu_id}")
#     if not data or not data.get("data"):
#         return None

#     item = data["data"]
#     attrs = item.get("attributes", {}) or {}

#     return {
#         "kitsu_id": item.get("id"),
#         "slug": attrs.get("slug"),
#         "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
#         "canonical_title": attrs.get("canonicalTitle", ""),
#         "titles": attrs.get("titles", {}) or {},
#         "synopsis": attrs.get("synopsis", "") or "",
#         "start_date": attrs.get("startDate", "") or "",
#         "end_date": attrs.get("endDate", "") or "",
#         "episode_count": attrs.get("episodeCount"),
#         "episode_length": attrs.get("episodeLength"),       # minutes per episode
#         "total_length": attrs.get("totalLength"),
#         "subtype": attrs.get("subtype", "") or "",
#         "status": attrs.get("status", "") or "",
#         "age_rating": attrs.get("ageRating", "") or "",
#         "poster": _poster_url(attrs.get("posterImage")),
#         "backdrop": _poster_url(attrs.get("coverImage"), "large"),
#         "rating": attrs.get("averageRating"),
#         "youtube_video_id": attrs.get("youtubeVideoId"),
#     }


# async def get_episode_by_number(kitsu_id: str | int, episode_number: int) -> dict | None:
#     """
#     Fetch a single episode by absolute number using Kitsu's filter[number].
#     This is the killer feature: anime filenames like [03] map directly here.
#     """
#     if not kitsu_id or not episode_number:
#         return None

#     data = await _kitsu_get(
#         "episodes",
#         params={
#             "filter[mediaId]": str(kitsu_id),
#             "filter[mediaType]": "Anime",
#             "filter[number]": str(episode_number),
#             "page[limit]": "1",
#         },
#     )
#     if not data or not data.get("data"):
#         return None

#     ep = data["data"][0]
#     attrs = ep.get("attributes", {}) or {}
#     return {
#         "id": ep.get("id"),
#         "number": attrs.get("number"),
#         "season_number": attrs.get("seasonNumber"),          # often None on anime
#         "relative_number": attrs.get("relativeNumber"),
#         "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
#         "canonical_title": attrs.get("canonicalTitle", ""),
#         "synopsis": attrs.get("synopsis", "") or "",
#         "air_date": attrs.get("airdate", "") or "",
#         "length": attrs.get("length"),                       # minutes
#         "thumbnail": _poster_url(attrs.get("thumbnail"), "original"),
#     }


# async def get_mappings(kitsu_id: str | int) -> dict:
#     """
#     Fetch Kitsu's external-ID mappings for an anime (MAL, AniList, AniDB,
#     TheTVDB, TheMovieDB, IMDb-ish via TVDB, etc.).

#     Returns a dict like:
#       {
#         "myanimelist/anime": "12345",
#         "anilist/anime":     "67890",
#         "thetvdb/series":    "999",
#         ...
#       }
#     """
#     if not kitsu_id:
#         return {}

#     data = await _kitsu_get(
#         f"anime/{kitsu_id}/mappings",
#         params={"page[limit]": "20"},
#     )
#     if not data or not data.get("data"):
#         return {}

#     mappings: dict[str, str] = {}
#     for item in data["data"]:
#         attrs = item.get("attributes", {}) or {}
#         site = attrs.get("externalSite")
#         ext_id = attrs.get("externalId")
#         if site and ext_id:
#             mappings[site] = str(ext_id)
#     return mappings
"""
Kitsu API helper — async client for the public Kitsu JSON:API.

Kitsu is an anime/manga discovery platform whose API natively supports
absolute episode numbering — exactly what anime release naming (e.g.
[Kaiyou] slamdunk [03]) gives us. Most public GET endpoints require no
auth.

Base URL: https://kitsu.io/api/edge
Docs:     https://hummingbird-me.github.io/api-docs/

This module returns plain dicts (the JSON:API `attributes` payload with
some normalisation), not heavyweight model classes, to keep integration
with the existing metadata pipeline simple.
"""

import asyncio
import aiohttp

from Backend.logger import LOGGER


KITSU_BASE = "https://kitsu.io/api/edge"
KITSU_HEADERS = {
    "Accept": "application/vnd.api+json",
    "Content-Type": "application/vnd.api+json",
}
KITSU_TIMEOUT = aiohttp.ClientTimeout(total=20)


# ----------------- Low-level fetch -----------------
async def _kitsu_get(path: str, params: dict | None = None) -> dict | None:
    """GET a Kitsu endpoint and return parsed JSON, or None on failure."""
    url = f"{KITSU_BASE}/{path.lstrip('/')}"
    try:
        async with aiohttp.ClientSession(
            headers=KITSU_HEADERS, timeout=KITSU_TIMEOUT
        ) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    LOGGER.warning(
                        f"Kitsu GET {url} returned status {resp.status}"
                    )
                    return None
                return await resp.json()
    except asyncio.TimeoutError:
        LOGGER.warning(f"Kitsu GET {url} timed out")
        return None
    except Exception as e:
        LOGGER.warning(f"Kitsu GET {url} failed: {e}")
        return None


# ----------------- Title normalisation -----------------
def _pick_title(titles: dict | None, canonical: str | None) -> str:
    """
    Choose the best display title from Kitsu's titles dict.
    Priority: English → romanised Japanese → canonical → first available.
    """
    titles = titles or {}
    for key in ("en", "en_us", "en_jp", "ja_jp"):
        val = titles.get(key)
        if val:
            return val
    if canonical:
        return canonical
    for val in titles.values():
        if val:
            return val
    return ""


def _poster_url(images: dict | None, size: str = "large") -> str:
    if not images:
        return ""
    return images.get(size) or images.get("original") or images.get("medium") or ""


# ----------------- Public API -----------------
async def search_anime(title: str, limit: int = 5) -> list[dict]:
    """
    Search Kitsu by title. Returns a list of simplified anime dicts.
    Uses filter[text] which performs a fuzzy match across known titles.
    """
    if not title:
        return []

    data = await _kitsu_get(
        "anime",
        params={
            "filter[text]": title,
            "page[limit]": str(limit),
            "fields[anime]": (
                "slug,titles,canonicalTitle,synopsis,startDate,endDate,"
                "episodeCount,episodeLength,subtype,posterImage,coverImage,"
                "status"
            ),
        },
    )
    if not data or not data.get("data"):
        return []

    results = []
    for item in data["data"]:
        attrs = item.get("attributes", {}) or {}
        results.append({
            "kitsu_id": item.get("id"),
            "slug": attrs.get("slug"),
            "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
            "canonical_title": attrs.get("canonicalTitle", ""),
            "titles": attrs.get("titles", {}) or {},
            "synopsis": attrs.get("synopsis", "") or "",
            "start_date": attrs.get("startDate", "") or "",
            "end_date": attrs.get("endDate", "") or "",
            "episode_count": attrs.get("episodeCount"),
            "episode_length": attrs.get("episodeLength"),
            "subtype": attrs.get("subtype", "") or "",  # TV, movie, OVA, ONA, special
            "status": attrs.get("status", "") or "",
            "poster": _poster_url(attrs.get("posterImage")),
            "backdrop": _poster_url(attrs.get("coverImage"), "large"),
        })
    return results


async def get_anime(kitsu_id: str | int) -> dict | None:
    """Fetch a single anime's full details by Kitsu ID."""
    if not kitsu_id:
        return None

    data = await _kitsu_get(f"anime/{kitsu_id}")
    if not data or not data.get("data"):
        return None

    item = data["data"]
    attrs = item.get("attributes", {}) or {}

    return {
        "kitsu_id": item.get("id"),
        "slug": attrs.get("slug"),
        "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
        "canonical_title": attrs.get("canonicalTitle", ""),
        "titles": attrs.get("titles", {}) or {},
        "synopsis": attrs.get("synopsis", "") or "",
        "start_date": attrs.get("startDate", "") or "",
        "end_date": attrs.get("endDate", "") or "",
        "episode_count": attrs.get("episodeCount"),
        "episode_length": attrs.get("episodeLength"),    # minutes per episode
        "total_length": attrs.get("totalLength"),
        "subtype": attrs.get("subtype", "") or "",
        "status": attrs.get("status", "") or "",
        "poster": _poster_url(attrs.get("posterImage")),
        "backdrop": _poster_url(attrs.get("coverImage"), "large"),
    }


async def get_episode_by_number(kitsu_id: str | int, episode_number: int) -> dict | None:
    """
    Fetch a single episode by absolute number using Kitsu's filter[number].
    This is the killer feature: anime filenames like [03] map directly here.
    """
    if not kitsu_id or not episode_number:
        return None

    data = await _kitsu_get(
        "episodes",
        params={
            "filter[mediaId]": str(kitsu_id),
            "filter[mediaType]": "Anime",
            "filter[number]": str(episode_number),
            "page[limit]": "1",
        },
    )
    if not data or not data.get("data"):
        return None

    ep = data["data"][0]
    attrs = ep.get("attributes", {}) or {}
    return {
        "id": ep.get("id"),
        "number": attrs.get("number"),
        "season_number": attrs.get("seasonNumber"),
        "relative_number": attrs.get("relativeNumber"),
        "title": _pick_title(attrs.get("titles"), attrs.get("canonicalTitle")),
        "canonical_title": attrs.get("canonicalTitle", ""),
        "synopsis": attrs.get("synopsis", "") or "",
        "air_date": attrs.get("airdate", "") or "",
        "length": attrs.get("length"),
    }


async def get_categories(kitsu_id: str | int) -> list[str]:
    """
    Fetch the category (genre-ish) titles for an anime.
    Kitsu uses 'categories' rather than 'genres' — they map cleanly enough
    for display purposes.
    """
    if not kitsu_id:
        return []

    data = await _kitsu_get(
        f"anime/{kitsu_id}/categories",
        params={"page[limit]": "20"},
    )
    if not data or not data.get("data"):
        return []

    categories: list[str] = []
    for item in data["data"]:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title")
        if title:
            categories.append(title)
    return categories
