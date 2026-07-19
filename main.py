"""
AethoWave Backend — FastAPI + yt-dlp
Deploy on Render (free tier)
AGGRESSIVE ANTI-BOT EVASION + FRESH EXTRACTION
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import yt_dlp
import httpx
import asyncio
import os
import json
import time
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

firebase_initialized = False
db = None

def init_firebase():
    global firebase_initialized, db
    if firebase_initialized:
        return
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
        sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if sa_json:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred, {
                "storageBucket": os.environ.get("FIREBASE_STORAGE_BUCKET", "")
            })
            db = firestore.client()
            firebase_initialized = True
            logger.info("Firebase initialized ✓")
    except Exception as e:
        logger.warning(f"Firebase init skipped: {e}")

init_firebase()

app = FastAPI(title="AethoWave API", version="1.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── AGGRESSIVE ANTI-BOT HEADERS ────────────────────────────
_YT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── YT-DLP OPTIONS ────────────────────────────────────────
YDL_SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "default_search": "ytsearch",
    "skip_download": True,
    "http_headers": _YT_HEADERS,
    "socket_timeout": 30,
    "retries": 5,
    "fragment_retries": 5,
    # Multiple client strategies
    "extractor_args": {
        "youtube": {
            "player_client": ["android", "mweb", "web"],
            "player_skip_js": False,
        }
    },
    "no_check_certificates": True,
}

YDL_STREAM_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    # CRITICAL: Try EVERY possible format
    "format": (
        "bestaudio[ext=webm]/"
        "bestaudio[ext=m4a]/"
        "bestaudio[ext=mp3]/"
        "bestaudio[acodec=opus]/"
        "bestaudio[acodec=vorbis]/"
        "bestaudio[acodec=aac]/"
        "bestaudio/"
        "best[ext=webm]/"
        "best[ext=mp4]/"
        "best"
    ),
    "http_headers": _YT_HEADERS,
    "socket_timeout": 30,
    "retries": 5,
    "fragment_retries": 5,
    # Multiple extraction strategies
    "extractor_args": {
        "youtube": {
            "player_client": ["android", "mweb", "web", "tv"],
            "player_skip_js": False,
            "sts": None,
            "api_key": None,
        }
    },
    "age_limit": None,  # Don't skip age-restricted
    "no_check_certificates": True,
    "geo_bypass": True,
    "geo_bypass_country": "US",
}

def fmt_dur(s: int) -> str:
    if not s:
        return "0:00"
    m, sec = divmod(int(s), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"

def format_track(entry: dict) -> dict:
    vid_id = entry.get("id", "")
    thumbnail = (
        entry.get("thumbnail")
        or (f"https://img.youtube.com/vi/{vid_id}/mqdefault.jpg" if vid_id else "")
    )
    duration = entry.get("duration") or 0
    artist = entry.get("uploader") or entry.get("channel") or "Unknown Artist"
    if artist.endswith(" - Topic"):
        artist = artist[:-8]
    return {
        "id": vid_id,
        "title": entry.get("title", "Unknown"),
        "artist": artist,
        "thumb": thumbnail,
        "duration": int(duration),
        "duration_str": fmt_dur(int(duration)),
        "views": entry.get("view_count"),
        "youtube_url": f"https://youtube.com/watch?v={vid_id}",
    }

# ── ROUTES ────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "AethoWave API online 🎵", "version": "1.3.0"}

@app.get("/health")
async def health():
    return {"ok": True, "firebase": firebase_initialized}

@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(15, le=25)):
    """Search YouTube for tracks"""
    try:
        search_query = f"ytsearch{limit}:{q} official audio"
        loop = asyncio.get_running_loop()

        def _search():
            with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
                info = ydl.extract_info(search_query, download=False)
                return info.get("entries", [])

        entries = await asyncio.wait_for(
            loop.run_in_executor(None, _search),
            timeout=25
        )
        results = []
        for entry in (entries or []):
            if not entry:
                continue
            dur = entry.get("duration") or 0
            if dur and dur > 600:
                continue
            results.append(format_track(entry))

        return {"results": results, "query": q, "count": len(results)}

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Search timeout")
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/{video_id}")
async def stream_url(video_id: str, background_tasks: BackgroundTasks):
    """
    Get streamable audio URL for a YouTube video.
    ALWAYS extracts fresh (no caching) because YouTube URLs expire quickly.
    """
    yt_url = f"https://youtube.com/watch?v={video_id}"
    loop = asyncio.get_running_loop()

    def _extract():
        logger.info(f"Extracting {video_id}...")
        with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
            return ydl.extract_info(yt_url, download=False)

    try:
        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract),
            timeout=30
        )
    except asyncio.TimeoutError:
        logger.error(f"Extraction timeout: {video_id}")
        raise HTTPException(status_code=504, detail="Extraction timeout")
    except yt_dlp.utils.DownloadError as e:
        err = str(e)
        logger.error(f"yt-dlp error for {video_id}: {err}")
        if "Video unavailable" in err or "Private video" in err:
            raise HTTPException(status_code=404, detail="Video unavailable")
        raise HTTPException(status_code=502, detail=f"Extraction failed")
    except Exception as e:
        logger.error(f"Extraction error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # ── SMART FORMAT SELECTION ─────────────────────────
    audio_url = None
    formats = info.get("formats") or []
    selected_fmt = None

    # Priority 1: Audio-only webm (best for streaming)
    for fmt in reversed(formats):
        if (fmt.get("acodec") != "none" and fmt.get("vcodec") == "none" and
            fmt.get("ext") == "webm"):
            audio_url = fmt.get("url")
            selected_fmt = f"webm({fmt.get('acodec')})"
            break

    # Priority 2: Audio-only m4a
    if not audio_url:
        for fmt in reversed(formats):
            if (fmt.get("acodec") != "none" and fmt.get("vcodec") == "none" and
                fmt.get("ext") == "m4a"):
                audio_url = fmt.get("url")
                selected_fmt = f"m4a({fmt.get('acodec')})"
                break

    # Priority 3: Any audio-only
    if not audio_url:
        for fmt in reversed(formats):
            if fmt.get("acodec") != "none" and fmt.get("vcodec") == "none":
                audio_url = fmt.get("url")
                selected_fmt = f"{fmt.get('ext')}({fmt.get('acodec')})"
                break

    # Priority 4: Best audio (may include video track, browser will ignore video)
    if not audio_url:
        for fmt in reversed(formats):
            if fmt.get("acodec") != "none":
                audio_url = fmt.get("url")
                selected_fmt = f"best({fmt.get('ext')})"
                break

    # Priority 5: Top-level URL
    if not audio_url:
        audio_url = info.get("url")
        selected_fmt = "top-level"

    if not audio_url:
        logger.error(f"No audio stream found for {video_id}")
        raise HTTPException(status_code=404, detail="No audio stream found")

    track_meta = format_track(info)
    logger.info(f"✓ Stream ready ({selected_fmt}): {video_id}")

    return {
        "url": audio_url,
        "proxy_url": f"/proxy-stream/{video_id}",
        "cached": False,
        "video_id": video_id,
        "track": track_meta,
        "expires_in": 21600,  # 6 hours (URL typically lasts 5-6h)
    }

@app.get("/trending")
async def trending(genre: str = Query("music", max_length=50), limit: int = Query(12, le=20)):
    """Get trending tracks for a genre"""
    query_map = {
        "pop": "top pop hits 2025 official audio",
        "hiphop": "best hip hop songs 2025 audio",
        "rnb": "best r&b songs 2025 audio",
        "electronic": "best electronic music 2025",
        "lofi": "lofi hip hop beats study chill",
        "rock": "best rock songs 2025 official",
        "jazz": "best jazz music instrumental",
        "classical": "best classical music orchestra",
        "indie": "best indie songs 2025 audio",
        "latin": "best latin hits 2025 audio",
        "downtempo": "downtempo chill music mix",
        "ambient": "ambient focus music instrumental",
        "acoustic": "acoustic songs morning playlist",
        "dance": "dance pop hits 2025",
    }
    search_q = query_map.get(genre.lower(), f"top {genre} music 2025")

    try:
        loop = asyncio.get_running_loop()

        def _search():
            with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
                info = ydl.extract_info(f"ytsearch{limit}:{search_q}", download=False)
                return info.get("entries", [])

        entries = await asyncio.wait_for(
            loop.run_in_executor(None, _search),
            timeout=25
        )
        results = []
        for e in (entries or []):
            if not e:
                continue
            dur = e.get("duration") or 0
            if dur and dur > 600:
                continue
            results.append(format_track(e))

        return {"results": results, "genre": genre, "count": len(results)}

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Trending timeout")
    except Exception as e:
        logger.error(f"Trending error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/proxy-stream/{video_id}")
async def proxy_stream(video_id: str, background_tasks: BackgroundTasks):
    """
    Proxy audio through server when direct CDN fails.
    ALWAYS re-extracts fresh to ensure URL is valid.
    """
    try:
        data = await stream_url(video_id, background_tasks)
        audio_url = data["url"]
    except Exception as e:
        logger.error(f"Failed to get URL for proxy {video_id}: {e}")
        raise HTTPException(status_code=502, detail="Could not extract stream")

    async def _stream():
        headers = {**_YT_HEADERS, "Range": "bytes=0-"}
        try:
            timeout = httpx.Timeout(60, connect=30)
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                limits=httpx.Limits(max_connections=10)
            ) as client:
                async with client.stream("GET", audio_url, headers=headers) as r:
                    if r.status_code != 200:
                        logger.error(f"Stream returned {r.status_code}")
                        return
                    bytes_sent = 0
                    async for chunk in r.aiter_bytes(chunk_size=65536):
                        if chunk:
                            yield chunk
                            bytes_sent += len(chunk)
                    logger.info(f"Proxy complete: {bytes_sent} bytes")
        except httpx.ReadTimeout:
            logger.error(f"Proxy read timeout")
        except Exception as e:
            logger.error(f"Proxy stream error: {e}")

    return StreamingResponse(
        _stream(),
        media_type="audio/mpeg",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "max-age=3600",
            "Content-Type": "audio/mpeg",
            "Access-Control-Allow-Origin": "*",
        },
    )

@app.get("/metadata/{video_id}")
async def metadata(video_id: str):
    """Get track metadata only"""
    try:
        yt_url = f"https://youtube.com/watch?v={video_id}"
        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
                return ydl.extract_info(yt_url, download=False)

        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract),
            timeout=20
        )
        return {"track": format_track(info)}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Metadata timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
