"""
Telugu TTS Web App - Fast chunked streaming version
"""
import asyncio
import edge_tts
import os
import re
import uuid
import json
import threading
import time
from flask import Flask, request, jsonify, send_file, render_template, Response, stream_with_context

app = Flask(__name__)

AUDIO_DIR = os.path.join(os.path.dirname(__file__), "static", "audio")
os.makedirs(AUDIO_DIR, exist_ok=True)

# Clean old files on startup (older than 1 hour)
def cleanup_old_files():
    try:
        now = time.time()
        for f in os.listdir(AUDIO_DIR):
            fp = os.path.join(AUDIO_DIR, f)
            if os.path.isfile(fp) and now - os.path.getmtime(fp) > 3600:
                os.remove(fp)
    except Exception:
        pass

cleanup_old_files()

VOICES = [
    {"id": "te-IN-ShrutiNeural", "name": "Shruti", "gender": "Female", "lang": "Telugu"},
    {"id": "te-IN-MohanNeural",  "name": "Mohan",  "gender": "Male",   "lang": "Telugu"},
]

PRESETS = [
    {"label": "Normal",        "rate": "+0%",  "pitch": "+0Hz"},
    {"label": "Slow & Clear",  "rate": "-15%", "pitch": "+0Hz"},
    {"label": "News Anchor",   "rate": "-10%", "pitch": "-10Hz"},
    {"label": "Deep & Serious","rate": "-10%", "pitch": "-20Hz"},
    {"label": "Friendly",      "rate": "-5%",  "pitch": "+15Hz"},
    {"label": "Fast",          "rate": "+15%", "pitch": "+0Hz"},
]

def split_chunks(text, max_len=300):
    """Text ని చిన్న chunks గా split చేస్తుంది"""
    # sentence boundaries మీద split
    parts = re.split(r'(?<=[.!?…।\n])\s*', text.strip())
    chunks, cur = [], ""
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if len(cur) + len(p) < max_len:
            cur = (cur + " " + p).strip()
        else:
            if cur:
                chunks.append(cur)
            cur = p
    if cur:
        chunks.append(cur)
    return chunks or [text]

async def generate_chunk(text, voice, rate, pitch, filepath):
    communicate = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch)
    await communicate.save(filepath)

@app.route("/")
def index():
    return render_template("index.html", voices=VOICES, presets=PRESETS)

@app.route("/generate_stream", methods=["POST"])
def generate_stream():
    """SSE stream — progress % పంపుతూ generate చేస్తుంది"""
    data  = request.json
    text  = data.get("text", "").strip()
    voice = data.get("voice", "te-IN-ShrutiNeural")
    rate  = data.get("rate",  "+0%")
    pitch = data.get("pitch", "+0Hz")

    if not text:
        return jsonify({"error": "Text రాయండి"}), 400

    chunks = split_chunks(text)
    total  = len(chunks)
    job_id = uuid.uuid4().hex

    def event_stream():
        tmp_files = []
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            for i, chunk in enumerate(chunks):
                tmp_path = os.path.join(AUDIO_DIR, f"{job_id}_{i}.mp3")
                tmp_files.append(tmp_path)

                # generate this chunk
                loop.run_until_complete(
                    generate_chunk(chunk, voice, rate, pitch, tmp_path)
                )

                pct = int((i + 1) / total * 90)
                yield f"data: {json.dumps({'progress': pct, 'step': i+1, 'total': total})}\n\n"

            # Merge all chunks into one file
            final_name = f"{job_id}.mp3"
            final_path = os.path.join(AUDIO_DIR, final_name)

            with open(final_path, "wb") as out:
                for tf in tmp_files:
                    with open(tf, "rb") as inp:
                        out.write(inp.read())

            # Delete temp chunk files
            for tf in tmp_files:
                try: os.remove(tf)
                except: pass

            loop.close()
            yield f"data: {json.dumps({'progress': 100, 'done': True, 'url': f'/static/audio/{final_name}', 'filename': final_name})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )

@app.route("/download/<filename>")
def download(filename):
    # safety check — no path traversal
    filename = os.path.basename(filename)
    filepath = os.path.join(AUDIO_DIR, filename)
    if not os.path.exists(filepath):
        return "File not found", 404
    return send_file(filepath, as_attachment=True, download_name="telugu_voice.mp3")

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    print("\n" + "="*50)
    print(f"  Telugu TTS App running on port {port}")
    print("="*50 + "\n")
    app.run(debug=False, host="0.0.0.0", port=port, threaded=True)
