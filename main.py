"""
AethoWave Backend v3 — FastAPI
Uses Piped API for stream URLs (no yt-dlp bot detection issues)
+ yt-dlp flat search for metadata only
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx
import asyncio
import os, json, logging, random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AethoWave API", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# Multiple Piped instances for fallback
PIPED_INSTANCES = [
    "https://pipedapi.kavin.rocks",
    "https://piped-api.privacy.com.de",
    "https://api.piped.projectsegfau.lt",
    "https://pipedapi.syncpundit.io",
    "https://piped.video/api",
]

YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "skip_download": True,
    "http_headers": {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
    },
}

def fmt_dur(s) -> str:
    try:
        s = int(s or 0)
    except:
        return ""
    if not s:
        return ""
    m, sec = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

def format_track(e: dict) -> dict:
    vid = e.get("id", "")
    artist = e.get("uploader") or e.get("channel") or "Unknown"
    if artist.endswith(" - Topic"):
        artist = artist[:-8]
    dur = e.get("duration") or 0
    return {
        "id": vid,
        "title": e.get("title", "Unknown"),
        "artist": artist,
        "thumb": e.get("thumbnail") or (f"https://img.youtube.com/vi/{vid}/mqdefault.jpg" if vid else ""),
        "duration": int(dur),
        "duration_str": fmt_dur(dur),
    }

async def yt_search(query: str, limit: int) -> list:
    loop = asyncio.get_running_loop()
    def _run():
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
            return (info or {}).get("entries", [])
    entries = await loop.run_in_executor(None, _run)
    results = []
    for e in (entries or []):
        if not e:
            continue
        dur = e.get("duration") or 0
        if dur and dur > 600:
            continue
        results.append(format_track(e))
    return results

async def get_stream_from_piped(video_id: str) -> dict:
    """Try each Piped instance until one works."""
    instances = PIPED_INSTANCES.copy()
    random.shuffle(instances)  # spread load

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for base in instances:
            try:
                url = f"{base}/streams/{video_id}"
                r = await client.get(url, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                })
                if r.status_code != 200:
                    continue
                data = r.json()

                # Pick best audio stream
                audio_streams = data.get("audioStreams", [])
                if not audio_streams:
                    continue

                # Sort by bitrate descending, prefer opus/m4a
                audio_streams.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
                best = audio_streams[0]

                # Build track metadata from Piped response
                track = {
                    "id": video_id,
                    "title": data.get("title", "Unknown"),
                    "artist": data.get("uploader", "Unknown"),
                    "thumb": data.get("thumbnailUrl") or f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg",
                    "duration": data.get("duration", 0),
                    "duration_str": fmt_dur(data.get("duration", 0)),
                }

                return {
                    "url": best["url"],
                    "mime_type": best.get("mimeType", "audio/webm"),
                    "bitrate": best.get("bitrate", 0),
                    "track": track,
                    "instance": base,
                }
            except Exception as e:
                logger.warning(f"Piped instance {base} failed: {e}")
                continue

    raise HTTPException(status_code=502, detail="All Piped instances failed")

@app.get("/")
async def root():
    return {"status": "AethoWave API v3 online", "version": "3.0.0"}

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(15, le=25)):
    try:
        results = await yt_search(f"{q} official audio", limit)
        return {"results": results, "query": q, "count": len(results)}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trending")
async def trending(genre: str = Query("music", max_length=50), limit: int = Query(12, le=20)):
    qmap = {
        "pop": "top pop hits 2025 official audio",
        "hiphop": "best hip hop 2025 audio",
        "rnb": "best r&b 2025 audio",
        "electronic": "best electronic music 2025",
        "lofi": "lofi hip hop beats study chill",
        "rock": "best rock songs 2025",
        "jazz": "jazz music instrumental",
        "classical": "classical music orchestra",
        "indie": "best indie songs 2025",
        "latin": "best latin hits 2025",
        "downtempo": "downtempo chill music",
        "ambient": "ambient focus instrumental",
        "acoustic": "acoustic songs playlist",
        "dance": "dance pop hits 2025",
    }
    q = qmap.get(genre.lower(), f"top {genre} music 2025")
    try:
        results = await yt_search(q, limit)
        return {"results": results, "genre": genre, "count": len(results)}
    except Exception as e:
        logger.error(f"Trending error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/{video_id}")
async def stream(video_id: str):
    """
    Get audio stream URL via Piped (no yt-dlp extraction, no bot detection).
    Returns { url, track, mime_type }
    """
    try:
        result = await get_stream_from_piped(video_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stream error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
