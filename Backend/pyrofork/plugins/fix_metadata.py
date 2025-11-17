import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.logger import LOGGER

CANCEL_REQUESTED = False

# -------------------------------
# Progress Bar Helper
# -------------------------------
def progress_bar(done, total, length=20):
    filled = int(length * (done / total)) if total else length
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"

# -------------------------------
# ETA Helper
# -------------------------------
def format_eta(seconds):
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"

# -------------------------------
# CANCEL BUTTON HANDLER
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_fix"))
async def cancel_fix(_, query):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = True
    await query.message.edit_text("❌ Metadata fixing has been cancelled by the user.")
    await query.answer("Cancelled")

# -------------------------------
# MAIN COMMAND
# -------------------------------
@Client.on_message(filters.command("fixmetadata") & filters.private & CustomFilters.owner, group=10)
async def fix_metadata_handler(_, message):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = False

    # Count total items
    total_movies = 0
    total_tv = 0

    for i in range(1, db.current_db_index + 1):
        key = f"storage_{i}"
        total_movies += await db.dbs[key]["movie"].count_documents({})
        total_tv += await db.dbs[key]["tv"].count_documents({})

    TOTAL = total_movies + total_tv
    DONE = 0
    start_time = time.time()

    status = await message.reply_text(
        "⏳ Initializing metadata fixing...",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_fix")]
        ])
    )

    CONCURRENCY = 20
    semaphore = asyncio.Semaphore(CONCURRENCY)

    # -------------------------
    # MOVIE UPDATE
    # -------------------------

    async def _safe_update_movie(collection, movie_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        async with semaphore:
            try:
                doc_id = movie_doc.get("_id")
                imdb_id = movie_doc.get("imdb_id")
                tmdb_id = movie_doc.get("tmdb_id")
                title = movie_doc.get("title")
                year = movie_doc.get("release_year")
                
                meta_1 = None
                meta_2 = None
                

                if imdb_id:
                    meta_1 = await fetch_movie_metadata(
                        title=title, encoded_string=None, year=year, quality=None, default_id=imdb_id
                    )
                    fetched_tmdb_id = meta_1.get("tmdb_id") if meta_1 else None
                    if tmdb_id or fetched_tmdb_id:
                        meta_2 = await fetch_movie_metadata(
                            title=title, encoded_string=None, year=year, quality=None, default_id=(tmdb_id or fetched_tmdb_id)
                        )

                elif tmdb_id:
                    meta_1 = await fetch_movie_metadata(
                        title=title, encoded_string=None, year=year, quality=None, default_id=tmdb_id
                    )
                    fetched_imdb_id = meta_1.get("imdb_id") if meta_1 else None
                    if fetched_imdb_id:
                        meta_2 = await fetch_movie_metadata(
                            title=title, encoded_string=None, year=year, quality=None, default_id=fetched_imdb_id
                        )
                else:
                    meta_1 = await fetch_movie_metadata(
                        title=title, encoded_string=None, year=year, quality=None, default_id=None
                    )

                    fetched_imdb_id = meta_1.get("imdb_id") if meta_1 else None
                    fetched_tmdb_id = meta_1.get("tmdb_id") if meta_1 else None

                    if fetched_imdb_id:
                         meta_2 = await fetch_movie_metadata(
                            title=title, encoded_string=None, year=year, quality=None, default_id=fetched_imdb_id
                        )
                    elif fetched_tmdb_id:
                        meta_2 = await fetch_movie_metadata(
                            title=title, encoded_string=None, year=year, quality=None, default_id=fetched_tmdb_id
                        )
                
                final_set_query = {}
                current_data = dict(movie_doc)
                api_map = {
                    "imdb_id": "imdb_id",
                    "tmdb_id": "tmdb_id",
                    "rate": "rating",
                    "cast": "cast",
                    "description": "description",
                    "genres": "genres",
                    "poster": "poster",
                    "backdrop": "backdrop",
                    "runtime": "runtime",
                    "logo": "logo"
                }
                for meta in [meta_1, meta_2]:
                    if not meta:
                        continue

                    for api_key, db_key in api_map.items():
                        new_val = meta.get(api_key)
                        is_empty = False
                        if db_key == "rating":
                            is_empty = not current_data.get(db_key) or current_data.get(db_key) == 0
                        else:
                            is_empty = not current_data.get(db_key)
                        if is_empty and new_val:
                            final_set_query[db_key] = new_val
                            current_data[db_key] = new_val
                
                if final_set_query:
                    if doc_id:
                        await collection.update_one(
                            {"_id": doc_id},
                            {"$set": final_set_query}
                        )
                    else:
                        await collection.update_one(
                            {"imdb_id": movie_doc.get("imdb_id")}, 
                            {"$set": final_set_query}
                        )

                DONE += 1

            except Exception as e:
                LOGGER.exception(f"Error updating movie {movie_doc.get('title')}: {e}")
                DONE += 1

    # -------------------------
    # TV UPDATE
    # -------------------------

    async def _safe_update_tv(collection, tv_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        async with semaphore:
            try:
                doc_id = tv_doc.get("_id")
                imdb_id = tv_doc.get("imdb_id")
                tmdb_id = tv_doc.get("tmdb_id")
                title = tv_doc.get("title")
                year = tv_doc.get("release_year")
                
                meta_1 = None
                meta_2 = None
                if imdb_id:
                    meta_1 = await fetch_tv_metadata(
                        title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=imdb_id
                    )
                    fetched_tmdb_id = meta_1.get("tmdb_id") if meta_1 else None
                    if tmdb_id or fetched_tmdb_id:
                        meta_2 = await fetch_tv_metadata(
                            title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=(tmdb_id or fetched_tmdb_id)
                        )
                elif tmdb_id:
                    meta_1 = await fetch_tv_metadata(
                        title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=tmdb_id
                    )
                    fetched_imdb_id = meta_1.get("imdb_id") if meta_1 else None
                    if fetched_imdb_id:
                        meta_2 = await fetch_tv_metadata(
                            title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=fetched_imdb_id
                        )
                else:
                    meta_1 = await fetch_tv_metadata(
                        title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=None # Title search
                    )
                    fetched_imdb_id = meta_1.get("imdb_id") if meta_1 else None
                    fetched_tmdb_id = meta_1.get("tmdb_id") if meta_1 else None

                    if fetched_imdb_id:
                         meta_2 = await fetch_tv_metadata(
                            title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=fetched_imdb_id
                        )
                    elif fetched_tmdb_id:
                        meta_2 = await fetch_tv_metadata(
                            title=title, season=1, episode=1, encoded_string=None, year=year, quality=None, default_id=fetched_tmdb_id
                        )

                final_set_query = {}
                current_data = dict(tv_doc)
                api_map = {
                    "imdb_id": "imdb_id",
                    "tmdb_id": "tmdb_id",
                    "rate": "rating",
                    "cast": "cast",
                    "description": "description",
                    "genres": "genres",
                    "poster": "poster",
                    "backdrop": "backdrop",
                    "runtime": "runtime",
                    "logo": "logo"
                }
                
                for meta in [meta_1, meta_2]:
                    if not meta:
                        continue

                    for api_key, db_key in api_map.items():
                        new_val = meta.get(api_key)
                        
                        is_empty = False
                        if db_key == "rating":
                            is_empty = not current_data.get(db_key) or current_data.get(db_key) == 0
                        else:
                            is_empty = not current_data.get(db_key)
                        
                        if is_empty and new_val:
                            final_set_query[db_key] = new_val
                            current_data[db_key] = new_val
                if final_set_query:
                    if doc_id:
                        await collection.update_one(
                            {"_id": doc_id},
                            {"$set": final_set_query}
                        )
                    else:
                        await collection.update_one(
                            {"imdb_id": tv_doc.get("imdb_id")},
                            {"$set": final_set_query}
                        )
                tasks = []

                final_imdb_id = current_data.get("imdb_id")
                
                if final_imdb_id:
                    for season in tv_doc.get("seasons", []):
                        s_num = season.get("season_number")

                        for ep in season.get("episodes", []):
                            e_num = ep.get("episode_number")

                            if ep.get("overview") and ep.get("released") and ep.get("episode_backdrop"):
                                continue

                            async def ep_task(s_local=s_num, e_local=e_num):
                                try:
                                    ep_meta = await fetch_tv_metadata(
                                        title=title,
                                        season=s_local,
                                        episode=e_local,
                                        encoded_string=None,
                                        year=year,
                                        quality=None,
                                        default_id=final_imdb_id  
                                    )

                                    if ep_meta:
                                        ep_update_query = {}
                                        if ep_meta.get("episode_overview"):
                                            ep_update_query["seasons.$[s].episodes.$[e].overview"] = ep_meta.get("episode_overview")
                                        if ep_meta.get("episode_released"):
                                            ep_update_query["seasons.$[s].episodes.$[e].released"] = ep_meta.get("episode_released")
                                        if ep_meta.get("episode_backdrop"):
                                            ep_update_query["seasons.$[s].episodes.$[e].episode_backdrop"] = ep_meta.get("episode_backdrop")
                                        
                                        if ep_update_query:
                                            filter_query = {"_id": doc_id} if doc_id else {"imdb_id": tv_doc.get("imdb_id")}
                                            await collection.update_one(
                                                filter_query,
                                                {"$set": ep_update_query},
                                                array_filters=[
                                                    {"s.season_number": s_local},
                                                    {"e.episode_number": e_local}
                                                ]
                                            )
                                except Exception as e:
                                    LOGGER.exception(f"Error updating episode {title} S{s_local}E{e_local}: {e}")

                            tasks.append(asyncio.create_task(ep_task()))

                    if tasks:
                        for i in range(0, len(tasks), CONCURRENCY):
                            await asyncio.gather(*tasks[i:i+CONCURRENCY], return_exceptions=True)

                DONE += 1

            except Exception as e:
                LOGGER.exception(f"Error updating TV show {tv_doc.get('title')}: {e}")
                DONE += 1
    # -------------------------
    # UPDATE MOVIES
    # -------------------------
    async def update_movies():
        tasks = []
        for i in range(1, db.current_db_index + 1):
            if CANCEL_REQUESTED:
                break

            collection = db.dbs[f"storage_{i}"]["movie"]
            cursor = collection.find({})

            async for movie in cursor:
                if CANCEL_REQUESTED:
                    break

                tasks.append(_safe_update_movie(collection, movie))

                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------
    # UPDATE TV
    # -------------------------
    async def update_tv():
        tasks = []
        for i in range(1, db.current_db_index + 1):

            collection = db.dbs[f"storage_{i}"]["tv"]
            cursor = collection.find({})

            async for tv in cursor:
                if CANCEL_REQUESTED:
                    break

                tasks.append(_safe_update_tv(collection, tv))

                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------
    # RUN ALL UPDATES
    # -------------------------
    try:
        await update_movies()
        if not CANCEL_REQUESTED:
            await update_tv()
    except Exception as e:
        LOGGER.exception(f"Error in fix_metadata run: {e}")

    if CANCEL_REQUESTED:
        return

    elapsed = time.time() - start_time
    await status.edit_text(
        f"🎉 **Metadata Fix Completed!**\n"
        f"{progress_bar(DONE, TOTAL)}\n"
        f"⏱ Time Taken: {format_eta(elapsed)}"
    )
