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

VOICE_EFFECTS = [
    {"id": "normal", "name": "Normal", "icon": "🎤", "description": "Clear natural voice"},
    {"id": "cave", "name": "Cave Echo", "icon": "🏔️", "description": "Deep cave reverb"},
    {"id": "underwater", "name": "Underwater", "icon": "🌊", "description": "Underwater bubbling effect"},
    {"id": "radio", "name": "Radio", "icon": "📻", "description": "Old radio transmission"},
    {"id": "robot", "name": "Robot", "icon": "🤖", "description": "Robotic voice modulation"},
    {"id": "whisper", "name": "Whisper", "icon": "🤫", "description": "Soft whispering effect"},
]

PRESETS = [
    {"label": "Normal",        "rate": "+0%",  "pitch": "+0Hz"},
    {"label": "Slow & Clear",  "rate": "-15%", "pitch": "+0Hz"},
    {"label": "News Anchor",   "rate": "-10%", "pitch": "-10Hz"},
    {"label": "Deep & Serious","rate": "-10%", "pitch": "-20Hz"},
    {"label": "Friendly",      "rate": "-5%",  "pitch": "+15Hz"},
    {"label": "Fast",          "rate": "+15%", "pitch": "+0Hz"},
]

def preprocess_telugu_text(text):
    """Telugu TTS కోసం numbers, years, dates సరిగ్గా convert చేస్తుంది"""
    
    # Years (1900-2099) - More comprehensive handling
    def replace_years(match):
        year = int(match.group())
        
        if year >= 1900 and year <= 1999:  # 1900s
            if year == 1900:
                return "వేయి తొమ్మిది వందలు"
            elif year <= 1909:
                unit = year % 10
                unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                return f"వేయి తొమ్మిది వందల {unit_words[unit]}"
            elif year <= 1919:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల పది"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల పదో {unit_words[unit]}"
            elif year <= 1929:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల ఇరవై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల ఇరవై {unit_words[unit]}"
            elif year <= 1939:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల ముప్పై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల ముప్పై {unit_words[unit]}"
            elif year <= 1949:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల నలభై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల నలభై {unit_words[unit]}"
            elif year <= 1959:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల యాభై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల యాభై {unit_words[unit]}"
            elif year <= 1969:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల అరవై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల అరవై {unit_words[unit]}"
            elif year <= 1979:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల డదబ్బై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల డదబ్బై {unit_words[unit]}"
            elif year <= 1989:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల ఎనభై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల ఎనభై {unit_words[unit]}"
            elif year <= 1999:
                unit = year % 10
                if unit == 0:
                    return "వేయి తొమ్మిది వందల తొంభై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"వేయి తొమ్మిది వందల తొంభై {unit_words[unit]}"
                    
        elif year >= 2000 and year <= 2099:  # 2000s
            if year == 2000:
                return "రెండు వేలు"
            elif year <= 2009:
                unit = year % 10
                unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                return f"రెండు వేల {unit_words[unit]}"
            elif year <= 2019:
                unit = year % 10
                if unit == 0:
                    return "రెండు వేల పది"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"రెండు వేల పదో {unit_words[unit]}"
            elif year <= 2029:
                unit = year % 10
                if unit == 0:
                    return "రెండు వేల ఇరవై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"రెండు వేల ఇరవై {unit_words[unit]}"
            elif year <= 2039:
                unit = year % 10
                if unit == 0:
                    return "రెండు వేల ముప్పై"
                else:
                    unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                    return f"రెండు వేల ముప్పై {unit_words[unit]}"
            else:  # 2040-2099
                decade = (year // 10) % 10
                unit = year % 10
                decade_words = ["", "", "ఇరవై", "ముప్పై", "నలభై", "యాభై", "అరవై", "డదబ్బై", "ఎనభై", "తొంభై"]
                unit_words = ["", "ఒకటి", "రెండు", "మూడు", "నాలుగు", "ఐదు", "ఆరు", "ఏడు", "ఎనిమిది", "తొమ్మిది"]
                
                if unit == 0:
                    return f"రెండు వేల {decade_words[decade]}"
                else:
                    return f"రెండు వేల {decade_words[decade]} {unit_words[unit]}"
        
        return match.group()  # Return original if not handled
    
    # Years pattern - match 4-digit years FIRST (before individual numbers)
    text = re.sub(r'\b(19[0-9]{2}|20[0-9]{2})\b', replace_years, text)
    
    # Then handle standalone single digits and small numbers  
    # But NOT if they're part of larger numbers or already processed years
    
    # Only replace standalone single digits 0-9 if they're not part of years or larger numbers
    text = re.sub(r'(?<!\d)\b0\b(?!\d)', 'సున్నా', text)
    text = re.sub(r'(?<!\d)\b1\b(?!\d)', 'ఒకటి', text)
    text = re.sub(r'(?<!\d)\b2\b(?!\d)', 'రెండు', text)
    text = re.sub(r'(?<!\d)\b3\b(?!\d)', 'మూడు', text)
    text = re.sub(r'(?<!\d)\b4\b(?!\d)', 'నాలుగు', text)
    text = re.sub(r'(?<!\d)\b5\b(?!\d)', 'ఐదు', text)
    text = re.sub(r'(?<!\d)\b6\b(?!\d)', 'ఆరు', text)
    text = re.sub(r'(?<!\d)\b7\b(?!\d)', 'ఏడు', text)
    text = re.sub(r'(?<!\d)\b8\b(?!\d)', 'ఎనిమిది', text)
    text = re.sub(r'(?<!\d)\b9\b(?!\d)', 'తొమ్మిది', text)
    
    # Common standalone numbers (10-99) - only if not part of years
    text = re.sub(r'(?<!\d)\b10\b(?!\d)', 'పది', text)
    text = re.sub(r'(?<!\d)\b11\b(?!\d)', 'పదకొండు', text)
    text = re.sub(r'(?<!\d)\b12\b(?!\d)', 'పన్నెండు', text)
    text = re.sub(r'(?<!\d)\b13\b(?!\d)', 'పదమూడు', text)
    text = re.sub(r'(?<!\d)\b14\b(?!\d)', 'పద్నాలుగు', text)
    text = re.sub(r'(?<!\d)\b15\b(?!\d)', 'పదిహేను', text)
    text = re.sub(r'(?<!\d)\b20\b(?!\d)', 'ఇరవై', text)
    text = re.sub(r'(?<!\d)\b25\b(?!\d)', 'ఇరవై ఐదు', text)
    text = re.sub(r'(?<!\d)\b30\b(?!\d)', 'ముప్పై', text)
    text = re.sub(r'(?<!\d)\b50\b(?!\d)', 'యాభై', text)
    text = re.sub(r'(?<!\d)\b100\b(?!\d)', 'వంద', text)
    text = re.sub(r'(?<!\d)\b1000\b(?!\d)', 'వేలు', text)
    
    # Currency amounts (రూపాయలు)
    text = re.sub(r'రూ\.(\d+)', r'రూపాయలు \1', text)
    text = re.sub(r'₹(\d+)', r'రూపాయలు \1', text)
    
    return text

def split_chunks(text, max_len=300):
    """Text ని చిన్న chunks గా split చేస్తుంది"""
    # First preprocess for Telugu TTS
    original_text = text
    text = preprocess_telugu_text(text)
    
    # Debug logging
    if "20" in original_text and original_text != text:
        print(f"DEBUG - Original: {original_text[:200]}...")
        print(f"DEBUG - Processed: {text[:200]}...")
    
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

def apply_voice_effect(input_file, output_file, effect):
    """Apply audio effects using FFmpeg"""
    try:
        import subprocess
        
        if effect == "normal":
            # No effect, just copy
            import shutil
            shutil.copy2(input_file, output_file)
            return True
            
        elif effect == "cave":
            # Cave echo - large reverb
            cmd = [
                "ffmpeg", "-i", input_file, "-af", 
                "aecho=0.8:0.9:1000:0.3,aecho=0.4:0.7:1800:0.25,volume=1.2",
                "-y", output_file
            ]
        elif effect == "underwater":
            # Underwater - low pass filter + bubbling
            cmd = [
                "ffmpeg", "-i", input_file, "-af", 
                "lowpass=f=800,aecho=0.5:0.7:100:0.1,volume=0.8",
                "-y", output_file
            ]
        elif effect == "radio":
            # Radio - band pass filter + static
            cmd = [
                "ffmpeg", "-i", input_file, "-af", 
                "bandpass=f=1000:width_type=h:w=800,volume=1.1",
                "-y", output_file
            ]
        elif effect == "robot":
            # Robot - pitch shift + chorus
            cmd = [
                "ffmpeg", "-i", input_file, "-af", 
                "asetrate=22050*1.2,aresample=22050,volume=0.9",
                "-y", output_file
            ]
        elif effect == "whisper":
            # Whisper - soft compression + quieter
            cmd = [
                "ffmpeg", "-i", input_file, "-af", 
                "compand=attacks=0.3:decays=0.8:points=-90/-900|-70/-70|-30/-9:volume=0.5",
                "-y", output_file
            ]
        else:
            # Fallback to normal
            import shutil
            shutil.copy2(input_file, output_file)
            return True
        
        # Run FFmpeg command
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"Effect processing failed: {e}")
        # Fallback - copy original file
        import shutil
        shutil.copy2(input_file, output_file)
        return False

@app.route("/")
def index():
    return render_template("index.html", voices=VOICES, presets=PRESETS, effects=VOICE_EFFECTS)

@app.route("/generate_stream", methods=["POST"])
def generate_stream():
    """SSE stream — progress % పంపుతూ generate చేస్తుంది"""
    data   = request.json
    text   = data.get("text", "").strip()
    voice  = data.get("voice", "te-IN-ShrutiNeural")
    rate   = data.get("rate",  "+0%")
    pitch  = data.get("pitch", "+0Hz")
    effect = data.get("effect", "normal")

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

                pct = int((i + 1) / total * 70)  # 70% for generation
                yield f"data: {json.dumps({'progress': pct, 'step': i+1, 'total': total})}\n\n"

            # Merge all chunks into one file
            raw_name = f"{job_id}_raw.mp3"
            raw_path = os.path.join(AUDIO_DIR, raw_name)

            with open(raw_path, "wb") as out:
                for tf in tmp_files:
                    with open(tf, "rb") as inp:
                        out.write(inp.read())

            # Delete temp chunk files
            for tf in tmp_files:
                try: os.remove(tf)
                except: pass

            # Apply voice effect
            yield f"data: {json.dumps({'progress': 80, 'step': 'Applying effects...'})}\n\n"
            
            final_name = f"{job_id}.mp3"
            final_path = os.path.join(AUDIO_DIR, final_name)
            
            success = apply_voice_effect(raw_path, final_path, effect)
            
            # Clean up raw file
            try: os.remove(raw_path)
            except: pass

            if not success:
                # If effect failed, use raw file
                os.rename(raw_path, final_path)

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
