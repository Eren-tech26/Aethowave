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
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FIREBASE INIT ──────────────────────────────────────────
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
app = FastAPI(title="AethoWave API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── YT-DLP OPTIONS ────────────────────────────────────────
_YT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
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
    "socket_timeout": 30,
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
}

YDL_STREAM_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    # NEW: Try multiple format strategies in order of preference
    "format": (
        "bestaudio[ext=webm]/"
        "bestaudio[ext=m4a]/"
        "bestaudio[ext=opus]/"
        "bestaudio/"
        "best"
    ),
    "http_headers": _YT_HEADERS,
    "socket_timeout": 30,
    "extractor_args": {"youtube": {"player_client": ["android", "web", "mweb"]}},
    "age_limit": 17,
    # NEW: Retry on network errors
    "retries": 3,
}

_mem_cache: dict = {}
CACHE_TTL = 10800  # 3 hours (shorter than 4 hours)

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

# ── CACHE HELPERS ──────────────────────────────────────────
async def get_cached_url(video_id: str) -> Optional[dict]:
    """Returns {url, metadata} or None if expired"""
    # Check memory first
    entry = _mem_cache.get(video_id)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        logger.info(f"Memory cache hit: {video_id}")
        return {"url": entry["url"], "metadata": entry.get("metadata")}

    # Check Firebase
    if not db:
        return None
    try:
        doc = db.collection("stream_cache").document(video_id).get()
        if doc.exists:
            data = doc.to_dict()
            cached_at = data.get("cached_at", 0)
            if isinstance(cached_at, datetime):
                cached_at = cached_at.timestamp()
            if time.time() - cached_at < CACHE_TTL:
                url = data.get("url")
                # Update memory cache
                _mem_cache[video_id] = {
                    "url": url,
                    "ts": cached_at,
                    "metadata": data.get("metadata")
                }
                logger.info(f"Firebase cache hit: {video_id}")
                return {"url": url, "metadata": data.get("metadata")}
            else:
                # Expired—delete it
                db.collection("stream_cache").document(video_id).delete()
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")
    return None

async def cache_url(video_id: str, url: str, metadata: dict):
    """Store URL in both memory and Firebase"""
    now = time.time()
    _mem_cache[video_id] = {"url": url, "ts": now, "metadata": metadata}
    if not db:
        return
    try:
        db.collection("stream_cache").document(video_id).set({
            "url": url,
            "cached_at": datetime.now(),
            "metadata": metadata,
            "expires_at": datetime.now() + timedelta(seconds=CACHE_TTL),
        })
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")

# ── ROUTES ────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "AethoWave API online 🎵", "version": "1.2.0"}

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
            timeout=20
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
async def stream_url(video_id: str, fresh: int = 0, background_tasks: BackgroundTasks):
    """
    Get streamable audio URL for a YouTube video.
    ?fresh=1 to skip cache and re-extract.
    Returns: {url, proxy_url, cached, video_id, track, expires_in}
    """
    # Check cache unless fresh=1
    if not fresh:
        cached = await get_cached_url(video_id)
        if cached:
            logger.info(f"Cache hit: {video_id}")
            return {
                "url": cached["url"],
                "proxy_url": f"/proxy-stream/{video_id}",
                "cached": True,
                "video_id": video_id,
                "track": cached["metadata"],
            }

    yt_url = f"https://youtube.com/watch?v={video_id}"
    loop = asyncio.get_running_loop()

    def _extract():
        with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
            return ydl.extract_info(yt_url, download=False)

    try:
        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract),
            timeout=25
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Extraction timeout")
    except yt_dlp.utils.DownloadError as e:
        err = str(e)
        logger.error(f"yt-dlp download error for {video_id}: {err}")
        if "Video unavailable" in err or "Private video" in err:
            raise HTTPException(status_code=404, detail="Video unavailable")
        raise HTTPException(status_code=502, detail=f"Extraction failed: {err}")
    except Exception as e:
        logger.error(f"Extraction error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # NEW: Better format selection logic
    audio_url = None
    formats = info.get("formats") or []
    
    # Strategy 1: Audio-only webm/m4a/opus
    for fmt in reversed(formats):
        if (fmt.get("acodec") != "none" and fmt.get("vcodec") == "none" and
            fmt.get("ext") in ("webm", "m4a", "opus")):
            audio_url = fmt.get("url")
            logger.info(f"Selected format: {fmt.get('ext')} ({fmt.get('acodec')})")
            break

    # Strategy 2: Any audio-only stream
    if not audio_url:
        for fmt in reversed(formats):
            if fmt.get("acodec") != "none" and fmt.get("vcodec") == "none":
                audio_url = fmt.get("url")
                logger.info(f"Selected audio-only: {fmt.get('ext')}")
                break

    # Strategy 3: Best audio (may have video)
    if not audio_url:
        for fmt in reversed(formats):
            if fmt.get("acodec") != "none":
                audio_url = fmt.get("url")
                logger.info(f"Selected with video: {fmt.get('ext')}")
                break

    # Strategy 4: Top-level URL
    if not audio_url:
        audio_url = info.get("url")
        logger.info("Using top-level URL")

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
            timeout=20
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
    Proxy audio through server when CDN URL fails.
    Streams in chunks to avoid timeouts.
    """
    audio_url = None
    
    # Try cache first
    cached = await get_cached_url(video_id)
    if cached:
        audio_url = cached["url"]
        logger.info(f"Using cached URL for proxy: {video_id}")
    
    # If not cached, extract fresh
    if not audio_url:
        try:
            data = await stream_url(video_id, background_tasks, fresh=1)
            audio_url = data["url"]
        except Exception as e:
            logger.error(f"Failed to get URL for proxy: {e}")
            raise HTTPException(status_code=502, detail="Could not get stream URL")

    async def _stream():
        headers = {**_YT_HEADERS, "Range": "bytes=0-"}
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True, limits=httpx.Limits(max_connections=5)) as client:
                async with client.stream("GET", audio_url, headers=headers) as r:
                    if r.status_code != 200:
                        logger.error(f"Stream returned {r.status_code}")
                        return
                    async for chunk in r.aiter_bytes(chunk_size=32768):
                        if chunk:
                            yield chunk
        except Exception as e:
            logger.error(f"Proxy stream error: {e}")

    return StreamingResponse(
        _stream(),
        media_type="audio/mpeg",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "max-age=3600",
            "Content-Type": "audio/mpeg",
        },
    )

@app.get("/metadata/{video_id}")
async def metadata(video_id: str):
    """Get track metadata without streaming URL"""
    try:
        yt_url = f"https://youtube.com/watch?v={video_id}"
        loop = asyncio.get_running_loop()

        def _extract():
            opts = {**YDL_STREAM_OPTS}
            opts["extract_flat"] = False
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(yt_url, download=False)

        info = await asyncio.wait_for(
            loop.run_in_executor(None, _extract),
            timeout=15
        )
        return {"track": format_track(info)}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Metadata timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
