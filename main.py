"""
AethoWave Backend v4 — FastAPI
yt-dlp for search + stream, proxied through backend to fix CORS
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import yt_dlp
import httpx
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AethoWave API", version="4.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

YDL_SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "skip_download": True,
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    },
}

YDL_STREAM_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio",
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
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
        with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
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

async def get_audio_url(video_id: str) -> dict:
    loop = asyncio.get_running_loop()
    def _run():
        with yt_dlp.YoutubeDL(YDL_STREAM_OPTS) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            if not info:
                raise Exception("No info returned")
            formats = info.get("formats", [])
            audio_formats = [f for f in formats if f.get("acodec") != "none" and f.get("vcodec") == "none"]
            if not audio_formats:
                audio_formats = [f for f in formats if f.get("acodec") != "none"]
            if not audio_formats:
                raise Exception("No audio format found")
            best = sorted(audio_formats, key=lambda f: f.get("abr") or f.get("tbr") or 0, reverse=True)[0]
            artist = info.get("uploader") or info.get("channel") or "Unknown"
            if artist.endswith(" - Topic"):
                artist = artist[:-8]
            return {
                "url": best["url"],
                "mime_type": best.get("ext", "webm"),
                "track": {
                    "id": video_id,
                    "title": info.get("title", "Unknown"),
                    "artist": artist,
                    "thumb": info.get("thumbnail") or f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg",
                    "duration": info.get("duration", 0),
                    "duration_str": fmt_dur(info.get("duration", 0)),
                }
            }
    return await loop.run_in_executor(None, _run)

@app.get("/")
async def root():
    return {"status": "AethoWave API v4 online", "version": "4.0.0"}

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
    try:
        data = await get_audio_url(video_id)
        audio_url = data["url"]
        track = data["track"]
        mime = data.get("mime_type", "webm")

        async def stream_audio():
            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                async with client.stream("GET", audio_url, headers={"User-Agent": "Mozilla/5.0", "Range": "bytes=0-"}) as r:
                    async for chunk in r.aiter_bytes(chunk_size=65536):
                        yield chunk

        return StreamingResponse(
            stream_audio(),
            media_type=f"audio/{mime}",
            headers={
                "X-Track-Title": track["title"],
                "X-Track-Artist": track["artist"],
                "X-Track-Thumb": track["thumb"],
                "X-Track-Duration": str(track["duration"]),
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-cache",
            }
        )
    except Exception as e:
        logger.error(f"Stream error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
