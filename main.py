"""
AethoWave Backend — FastAPI + yt-dlp
Deploy on Render (free tier)
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import yt_dlp
import httpx
import asyncio
import hashlib
import os
import json
import time
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FIREBASE INIT (optional) ───────────────────────────────
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

# ── APP ────────────────────────────────────────────────────
app = FastAPI(title="AethoWave API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins — restrict after you know your Vercel URL
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── YT-DLP OPTIONS ────────────────────────────────────────
# These headers + options help bypass YouTube bot detection
_YT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

YDL_SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "default_search": "ytsearch",
    "skip_download": True,
    "http_headers": _YT_HEADERS,
    # Use Android client — less bot detection than web client
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
}

YDL_STREAM_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    # Prefer opus/m4a audio-only streams
    "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
    "http_headers": _YT_HEADERS,
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    # Avoid age-restricted / DRM content
    "age_limit": 17,
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
    # Clean up auto-generated channel names like "Artist - Topic"
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

# ── CACHE ─────────────────────────────────────────────────
_mem_cache: dict = {}  # fallback in-memory cache (lost on restart)

async def get_cached_url(video_id: str) -> Optional[str]:
    # In-memory first
    entry = _mem_cache.get(video_id)
    if entry and time.time() - entry["ts"] < 14400:
        return entry["url"]

    if not db:
        return None
    try:
        doc = db.collection("stream_cache").document(video_id).get()
        if doc.exists:
            data = doc.to_dict()
            if time.time() - data.get("cached_at", 0) < 14400:
                url = data.get("url")
                _mem_cache[video_id] = {"url": url, "ts": data["cached_at"]}
                return url
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
    return None

async def cache_url(video_id: str, url: str, metadata: dict):
    _mem_cache[video_id] = {"url": url, "ts": time.time()}
    if not db:
        return
    try:
        db.collection("stream_cache").document(video_id).set({
            "url": url,
            "cached_at": time.time(),
            **metadata,
        })
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")

# ── ROUTES ────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "AethoWave API online 🎵", "version": "1.1.0"}

@app.get("/health")
async def health():
    return {"ok": True, "firebase": firebase_initialized}


@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(15, le=25)):
    """Search YouTube for tracks. GET /search?q=lofi+beats&limit=15"""
    try:
        search_query = f"ytsearch{limit}:{q} official audio"
        loop = asyncio.get_running_loop()

        def _search():
            with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
                info = ydl.extract_info(search_query, download=False)
                return info.get("entries", [])

        entries = await loop.run_in_executor(None, _search)
        results = []
        for entry in (entries or []):
            if not entry:
                continue
            dur = entry.get("duration") or 0
            # Skip videos longer than 10 min (likely not songs)
            if dur and dur > 600:
                continue
            results.append(format_track(entry))

        return {"results": results, "query": q, "count": len(results)}

    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stream/{video_id}")
async def stream_url(video_id: str, background_tasks: BackgroundTasks):
    """
    Get a streamable audio URL for a YouTube video.
    GET /stream/dQw4w9WgXcQ
    Returns: { url, proxy_url, expires_in, track }
    
    The response now also includes `proxy_url` — use it as a fallback
    if the direct CDN URL fails due to CORS or expiry.
    """
    cached = await get_cached_url(video_id)
    if cached:
        logger.info(f"Cache hit: {video_id}")
        return {
            "url": cached,
            "proxy_url": f"/proxy-stream/{video_id}",
            "cached": True,
            "video_id": video_id,
        }

    yt_url = f"https://youtube.com/watch?v={video_id}"
    loop = asyncio.get_running_loop()

    def _extract():
        with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
            return ydl.extract_info(yt_url, download=False)

    try:
        info = await loop.run_in_executor(None, _extract)
    except yt_dlp.utils.DownloadError as e:
        err = str(e)
        logger.error(f"yt-dlp download error for {video_id}: {err}")
        if "Video unavailable" in err or "Private video" in err:
            raise HTTPException(status_code=404, detail="Video unavailable")
        raise HTTPException(status_code=502, detail=f"Extraction failed: {err}")
    except Exception as e:
        logger.error(f"Unexpected extraction error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # Pick best audio-only format
    audio_url = None
    formats = info.get("formats") or []

    # Prefer audio-only streams (no video track)
    for fmt in reversed(formats):
        if fmt.get("acodec") != "none" and fmt.get("vcodec") == "none":
            if fmt.get("ext") in ("webm", "m4a", "opus"):
                audio_url = fmt.get("url")
                break

    # Fall back to any audio
    if not audio_url:
        for fmt in reversed(formats):
            if fmt.get("acodec") != "none":
                audio_url = fmt.get("url")
                break

    # Last resort — top-level url
    if not audio_url:
        audio_url = info.get("url")

    if not audio_url:
        raise HTTPException(status_code=404, detail="No audio stream found")

    track_meta = format_track(info)
    background_tasks.add_task(cache_url, video_id, audio_url, track_meta)

    return {
        "url": audio_url,
        "proxy_url": f"/proxy-stream/{video_id}",
        "cached": False,
        "video_id": video_id,
        "track": track_meta,
        "expires_in": 14400,
    }


@app.get("/trending")
async def trending(genre: str = Query("music", max_length=50), limit: int = Query(12, le=20)):
    """Get trending tracks for a genre. GET /trending?genre=lofi&limit=12"""
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

        entries = await loop.run_in_executor(None, _search)
        results = []
        for e in (entries or []):
            if not e:
                continue
            dur = e.get("duration") or 0
            if dur and dur > 600:
                continue
            results.append(format_track(e))

        return {"results": results, "genre": genre, "count": len(results)}

    except Exception as e:
        logger.error(f"Trending error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/proxy-stream/{video_id}")
async def proxy_stream(video_id: str, background_tasks: BackgroundTasks):
    """
    Proxy audio through the server — use when direct CDN URL fails (CORS / expiry).
    GET /proxy-stream/dQw4w9WgXcQ
    """
    # Get (possibly cached) stream URL
    audio_url = await get_cached_url(video_id)
    if not audio_url:
        data = await stream_url(video_id, background_tasks)
        audio_url = data["url"]

    async def _stream():
        headers = {**_YT_HEADERS, "Range": "bytes=0-"}
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            async with client.stream("GET", audio_url, headers=headers) as r:
                async for chunk in r.aiter_bytes(chunk_size=16384):
                    yield chunk

    return StreamingResponse(
        _stream(),
        media_type="audio/webm",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        },
    )


@app.get("/metadata/{video_id}")
async def metadata(video_id: str):
    """Get track metadata only. GET /metadata/dQw4w9WgXcQ"""
    try:
        yt_url = f"https://youtube.com/watch?v={video_id}"
        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
                return ydl.extract_info(yt_url, download=False)

        info = await loop.run_in_executor(None, _extract)
        return {"track": format_track(info)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

