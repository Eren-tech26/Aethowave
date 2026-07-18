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
import firebase_admin
from firebase_admin import credentials, firestore, storage
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FIREBASE INIT ──────────────────────────────────────────
# On Render: set FIREBASE_SERVICE_ACCOUNT env var as JSON string
firebase_initialized = False
db = None
bucket = None

def init_firebase():
    global firebase_initialized, db, bucket
    if firebase_initialized:
        return
    try:
        sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if sa_json:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred, {
                "storageBucket": os.environ.get("FIREBASE_STORAGE_BUCKET", "")
            })
            db = firestore.client()
            bucket = storage.bucket()
            firebase_initialized = True
            logger.info("Firebase initialized ✓")
    except Exception as e:
        logger.warning(f"Firebase init skipped: {e}")

init_firebase()

# ── APP ────────────────────────────────────────────────────
app = FastAPI(title="AethoWave API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://aethowave.vercel.app"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── YT-DLP HELPERS ────────────────────────────────────────
YDL_SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "default_search": "ytsearch",
    "skip_download": True,
}

YDL_INFO_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
    "extract_flat": False,
}

def track_id(video_id: str) -> str:
    return hashlib.md5(video_id.encode()).hexdigest()[:12]

def format_track(entry: dict) -> dict:
    vid_id = entry.get("id", "")
    thumbnail = (
        entry.get("thumbnail")
        or f"https://img.youtube.com/vi/{vid_id}/mqdefault.jpg"
    )
    duration = entry.get("duration") or 0
    return {
        "id": vid_id,
        "title": entry.get("title", "Unknown"),
        "artist": entry.get("uploader") or entry.get("channel") or "Unknown Artist",
        "thumb": thumbnail,
        "duration": int(duration),
        "duration_str": fmt_dur(int(duration)),
        "views": entry.get("view_count"),
        "youtube_url": f"https://youtube.com/watch?v={vid_id}",
    }

def fmt_dur(s: int) -> str:
    m, sec = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"

# ── CACHE HELPERS ────────────────────────────────────────
async def get_cached_url(video_id: str) -> Optional[str]:
    """Check Firestore for a cached stream URL."""
    if not db:
        return None
    try:
        doc = db.collection("stream_cache").document(video_id).get()
        if doc.exists:
            data = doc.to_dict()
            if time.time() - data.get("cached_at", 0) < 14400:
                return data.get("url")
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
    return None

async def cache_url(video_id: str, url: str, metadata: dict):
    """Store stream URL + metadata in Firestore."""
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
    return {"status": "AethoWave API online 🎵", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"ok": True, "firebase": firebase_initialized}


@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(15, le=25)):
    """
    Search YouTube for tracks.
    GET /search?q=lofi+beats&limit=15
    """
    try:
        results = []
        search_query = f"ytsearch{limit}:{q}"

        loop = asyncio.get_running_loop()
        def _search():
            with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
                info = ydl.extract_info(search_query, download=False)
                return info.get("entries", [])

        entries = await loop.run_in_executor(None, _search)

        for entry in entries:
            if not entry:
                continue
            dur = entry.get("duration") or 0
            if dur > 600:
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
    Returns: { url, expires_in, track }
    """
    cached = await get_cached_url(video_id)
    if cached:
        logger.info(f"Cache hit: {video_id}")
        return {"url": cached, "cached": True, "video_id": video_id}

    try:
        yt_url = f"https://youtube.com/watch?v={video_id}"
        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(YDL_INFO_OPTS) as ydl:
                info = ydl.extract_info(yt_url, download=False)
                return info

        info = await loop.run_in_executor(None, _extract)

        audio_url = None
        formats = info.get("formats", [])

        for fmt in reversed(formats):
            if fmt.get("acodec") != "none" and fmt.get("vcodec") == "none":
                ext = fmt.get("ext", "")
                if ext in ("webm", "m4a", "opus"):
                    audio_url = fmt.get("url")
                    break

        if not audio_url:
            for fmt in reversed(formats):
                if fmt.get("acodec") != "none":
                    audio_url = fmt.get("url")
                    break

        if not audio_url:
            raise HTTPException(status_code=404, detail="No audio stream found")

        track_meta = format_track(info)
        background_tasks.add_task(cache_url, video_id, audio_url, track_meta)

        return {
            "url": audio_url,
            "cached": False,
            "video_id": video_id,
            "track": track_meta,
            "expires_in": 14400,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stream extraction error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not extract audio: {str(e)}")


@app.get("/trending")
async def trending(genre: str = Query("music", max_length=50), limit: int = Query(12, le=20)):
    """
    Get trending tracks for a genre.
    GET /trending?genre=lofi&limit=12
    """
    query_map = {
        "pop": "top pop hits 2025",
        "hiphop": "best hip hop songs 2025",
        "rnb": "best r&b songs 2025",
        "electronic": "best electronic music 2025",
        "lofi": "lofi hip hop beats study",
        "rock": "best rock songs 2025",
        "jazz": "best jazz music",
        "classical": "best classical music",
        "indie": "best indie songs 2025",
        "latin": "best latin hits 2025",
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
        for e in entries:
            if not e:
                continue
            dur = e.get("duration") or 0
            if dur > 600:
                continue
            results.append(format_track(e))

        return {"results": results, "genre": genre, "count": len(results)}

    except Exception as e:
        logger.error(f"Trending error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metadata/{video_id}")
async def metadata(video_id: str):
    """
    Get track metadata + thumbnail only (no stream URL).
    GET /metadata/dQw4w9WgXcQ
    """
    try:
        yt_url = f"https://youtube.com/watch?v={video_id}"
        loop = asyncio.get_running_loop()
        def _extract():
            opts = {**YDL_INFO_OPTS, "skip_download": True}
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(yt_url, download=False)

        info = await loop.run_in_executor(None, _extract)
        return {"track": format_track(info)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/proxy-stream/{video_id}")
async def proxy_stream(video_id: str, background_tasks: BackgroundTasks):
    """
    Proxy the audio stream through the server (handles CORS on some yt URLs).
    Only use if direct URL fails due to CORS.
    """
    stream_data = await stream_url(video_id, background_tasks)
    audio_url = stream_data["url"]

    async def stream_audio():
        async with httpx.AsyncClient(timeout=30) as client:
            async with client.stream("GET", audio_url) as r:
                async for chunk in r.aiter_bytes(chunk_size=8192):
                    yield chunk

    return StreamingResponse(
        stream_audio(),
        media_type="audio/webm",
        headers={"Accept-Ranges": "bytes"},
    )
