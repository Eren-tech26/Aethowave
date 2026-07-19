from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import yt_dlp
import httpx
import asyncio
import logging, random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AethoWave API", version="5.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

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

async def get_stream_direct(video_id: str) -> dict:
    loop = asyncio.get_running_loop()
    def _run():
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio",
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            },
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False
            )
            url = info.get("url")
            if not url and info.get("requested_formats"):
                url = info["requested_formats"][0].get("url")
            return {
                "url": url,
                "mime_type": "audio/webm",
                "title": info.get("title", "Unknown"),
                "artist": info.get("uploader", "Unknown"),
                "duration": info.get("duration", 0),
                "duration_str": fmt_dur(info.get("duration", 0)),
                "thumb": f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg",
            }
    return await loop.run_in_executor(None, _run)

@app.get("/")
async def root():
    return {"status": "AethoWave API v5 online", "version": "5.0.0"}

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
        info = await get_stream_direct(video_id)
        return info
    except Exception as e:
        logger.error(f"Stream error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/proxy/{video_id}")
async def proxy_stream(video_id: str):
    try:
        info = await get_stream_direct(video_id)
        stream_url = info["url"]
        if not stream_url:
            raise HTTPException(status_code=502, detail="No stream URL found")

        async def generate():
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                async with client.stream("GET", stream_url, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://www.youtube.com/",
                }) as r:
                    async for chunk in r.aiter_bytes(8192):
                        yield chunk

        return StreamingResponse(generate(), media_type=info["mime_type"], headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Proxy error for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
