"""
PP Script Timecoder — Web App
Flask backend: upload script + video → timecoded .docx via OpenAI Whisper API
"""

import os, re, uuid, json, time, shutil, tempfile, threading, subprocess
from pathlib import Path
from difflib import SequenceMatcher

from flask import (Flask, request, jsonify, send_file,
                   render_template, Response)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024 * 1024  # 2 GB upload limit

# ── Job store (in-memory — fine for single-server use) ────────────────────────
jobs = {}   # job_id → {"status", "progress", "log", "output_path", "error"}


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize(text):
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def similarity(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def seconds_to_tc(secs, offset_secs=0.0, fps=25):
    total = max(0.0, secs + offset_secs)
    h  = int(total // 3600)
    m  = int((total % 3600) // 60)
    s  = int(total % 60)
    ff = int(round((total - int(total)) * fps))
    if ff >= fps:
        ff = fps - 1
    return f"{h:02d}:{m:02d}:{s:02d}:{ff:02d}"

def tc_to_seconds(tc):
    parts = tc.strip().replace(";", ":").split(":")
    try:
        if len(parts) == 4:
            return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2]) + int(parts[3])/25.0
        elif len(parts) == 3:
            return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
    except:
        pass
    return 0.0

def dur_str(secs):
    if secs <= 0:
        return ""
    m  = int(secs // 60)
    s  = int(secs % 60)
    ff = int(round((secs - int(secs)) * 25))
    return f"{m:01d}:{s:02d}:{ff:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# Script parser  (same logic as the desktop app)
# ─────────────────────────────────────────────────────────────────────────────

def parse_source_script(docx_path):
    from docx import Document

    doc   = Document(str(docx_path))
    lines = []

    def is_bold(para):
        return any(r.bold for r in para.runs if r.text.strip())

    def is_italic(para):
        return any(r.italic for r in para.runs if r.text.strip())

    def classify_cell(cell):
        results = []
        paras   = cell.paragraphs
        i       = 0
        while i < len(paras):
            para = paras[i]
            text = para.text.strip()
            if not text:
                i += 1; continue

            bold   = is_bold(para)
            italic = is_italic(para)

            if bold and text == text.upper() and len(text) > 3:
                block = [text]
                while i + 1 < len(paras):
                    nxt = paras[i + 1]
                    nt  = nxt.text.strip()
                    if nt and is_bold(nxt) and nt == nt.upper():
                        block.append(nt); i += 1
                    else:
                        break
                results.append({"type": "vo", "speaker": None, "text": " / ".join(block)})

            elif bold and not italic:
                tup = text.upper()
                if any(kw in tup for kw in [
                    "PART ", "ACT ", "WEIGHT LOSS", "COLD OPEN", "TITLES",
                    "CODA", "RECAP", "COLD", "TEASER", "TAG", "BEAT"]):
                    results.append({"type": "part",    "speaker": None, "text": text})
                elif len(text.split()) <= 6:
                    results.append({"type": "section", "speaker": None, "text": text})
                else:
                    results.append({"type": "vo",      "speaker": None, "text": text})

            elif italic or re.match(r'^[A-Z][A-Z\s\.\-]+:', text):
                m = re.match(r'^([A-Z][A-Z\s\.\-]+):\s*(.*)', text)
                if m:
                    speaker = m.group(1).strip()
                    diag    = m.group(2).strip()
                    block   = [diag] if diag else []
                    while i + 1 < len(paras):
                        nxt = paras[i + 1]
                        nt  = nxt.text.strip()
                        if not nt or re.match(r'^[A-Z][A-Z\s\.\-]+:', nt) or (is_bold(nxt) and nt == nt.upper()):
                            break
                        block.append(nt); i += 1
                    results.append({"type": "act", "speaker": speaker,
                                    "text": " ".join(block)})
                else:
                    results.append({"type": "act", "speaker": None, "text": text})
            else:
                results.append({"type": "act", "speaker": None, "text": text})
            i += 1
        return results

    for table in doc.tables:
        for row in table.rows:
            if not row.cells:
                continue
            cell = row.cells[-1] if len(row.cells) >= 2 else row.cells[0]
            ct   = cell.text.strip().upper()
            if ct in ("SCRIPT & VO", "SCRIPT", "VO", "CONTENT", ""):
                continue
            lines.extend(classify_cell(cell))

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue
        if is_bold(para) and len(text) > 5:
            lines.append({"type": "vo",  "speaker": None, "text": text})
        elif text:
            lines.append({"type": "act", "speaker": None, "text": text})

    return lines


# ─────────────────────────────────────────────────────────────────────────────
# Audio extraction  (ffmpeg — available on Railway via Dockerfile)
# ─────────────────────────────────────────────────────────────────────────────

def extract_audio(video_path, out_wav):
    """Extract mono 16 kHz WAV from video. Returns path to wav file."""
    cmd = [
        "ffmpeg", "-y", "-i", str(video_path),
        "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
        str(out_wav)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr[-800:]}")
    return out_wav


def compress_audio(wav_path, out_mp3):
    """Compress WAV → MP3 at 64kbps to stay under OpenAI 25 MB limit."""
    cmd = [
        "ffmpeg", "-y", "-i", str(wav_path),
        "-b:a", "64k", str(out_mp3)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg compress failed:\n{result.stderr[-400:]}")
    return out_mp3


# ─────────────────────────────────────────────────────────────────────────────
# Whisper API transcription
# ─────────────────────────────────────────────────────────────────────────────

def transcribe_openai(audio_path, api_key):
    """Call OpenAI Whisper API. Returns list of {start, end, text} segments."""
    from openai import OpenAI

    client  = OpenAI(api_key=api_key)
    size_mb = Path(audio_path).stat().st_size / 1024 / 1024

    if size_mb > 24:
        raise RuntimeError(
            f"Audio file is {size_mb:.1f} MB — exceeds OpenAI's 25 MB limit. "
            "Try a shorter programme or lower-bitrate export."
        )

    with open(audio_path, "rb") as f:
        response = client.audio.transcriptions.create(
            model        = "whisper-1",
            file         = f,
            response_format = "verbose_json",
            timestamp_granularities = ["segment"]
        )

    segments = []
    for seg in response.segments:
        segments.append({
            "start": float(seg.start),
            "end":   float(seg.end),
            "text":  seg.text.strip()
        })
    return segments


# ─────────────────────────────────────────────────────────────────────────────
# Timecode matching
# ─────────────────────────────────────────────────────────────────────────────

def match_timecodes(script_lines, segments, tc_offset_secs, fps):
    if not segments:
        return [{**l, "tc_in": "", "tc_out": "", "dur": ""} for l in script_lines]

    results    = []
    seg_cursor = 0
    WINDOW     = 8
    LOOKAHEAD  = 60
    THRESHOLD  = 0.28
    n_segs     = len(segments)

    for line in script_lines:
        text = line.get("text", "").strip()

        if line["type"] in ("section", "part", "coda") or not text:
            results.append({**line, "tc_in": "", "tc_out": "", "dur": ""})
            continue

        norm_line  = normalize(text)
        best_score = 0.0
        best_i = best_j = None
        search_end = min(seg_cursor + LOOKAHEAD, n_segs)

        for i in range(seg_cursor, search_end):
            joined = ""
            for j in range(i, min(i + WINDOW, search_end)):
                joined     = (joined + " " + segments[j]["text"]).strip()
                norm_joined = normalize(joined)
                score      = similarity(norm_line, norm_joined)
                if len(norm_line) > 8 and norm_line in norm_joined:
                    score = max(score, 0.75)
                if score > best_score:
                    best_score = score
                    best_i, best_j = i, j

        if best_score >= THRESHOLD and best_i is not None:
            seg_cursor = best_i
            t_in  = segments[best_i]["start"]
            t_out = segments[best_j]["end"]
            results.append({
                **line,
                "tc_in":  seconds_to_tc(t_in,  tc_offset_secs, fps),
                "tc_out": seconds_to_tc(t_out, tc_offset_secs, fps),
                "dur":    dur_str(t_out - t_in),
            })
        else:
            results.append({**line, "tc_in": "", "tc_out": "", "dur": ""})

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Output .docx builder
# ─────────────────────────────────────────────────────────────────────────────

def build_output_docx(matched_lines, output_path, fps):
    from docx import Document
    from docx.shared import Pt, RGBColor, Inches
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_ALIGN_VERTICAL

    doc     = Document()
    section = doc.sections[0]
    section.page_width    = Inches(8.5)
    section.page_height   = Inches(11)
    section.left_margin   = Inches(0.75)
    section.right_margin  = Inches(0.75)
    section.top_margin    = Inches(0.75)
    section.bottom_margin = Inches(0.75)

    def set_bg(cell, hex_color):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd  = OxmlElement("w:shd")
        shd.set(qn("w:val"),   "clear")
        shd.set(qn("w:color"), "auto")
        shd.set(qn("w:fill"),  hex_color.replace("#", ""))
        tcPr.append(shd)

    def set_borders(cell, color="CCCCCC"):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        tcB  = OxmlElement("w:tcBorders")
        for side in ("top", "left", "bottom", "right"):
            t = OxmlElement(f"w:{side}")
            t.set(qn("w:val"),   "single")
            t.set(qn("w:sz"),    "4")
            t.set(qn("w:color"), color)
            tcB.append(t)
        tcPr.append(tcB)

    def run(para, text, bold=False, italic=False, color=None, size=9, mono=False):
        r = para.add_run(text)
        r.bold   = bold
        r.italic = italic
        r.font.size = Pt(size)
        r.font.name = "Courier New" if mono else "Arial"
        if color:
            rgb = tuple(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
            r.font.color.rgb = RGBColor(*rgb)

    # Title
    tp = doc.add_paragraph()
    run(tp, "POST-PRODUCTION SCRIPT", bold=True, size=14)
    tp2 = doc.add_paragraph()
    run(tp2, f"Generated by PP Timecoder  ·  {fps}fps", size=9, color="#888888")
    doc.add_paragraph()

    W_TC = 1008; W_DUR = 720; W_SCR = 4824
    table = doc.add_table(rows=0, cols=4)
    table.style = "Table Grid"

    # Header row
    hrow = table.add_row()
    for cell, label in zip(hrow.cells, ["TC IN", "TC OUT", "DUR", "SCRIPT & VO"]):
        set_bg(cell, "BDD7EE"); set_borders(cell, "AAAAAA")
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run(p, label, bold=True, size=8)

    for line in matched_lines:
        ltype  = line.get("type", "act")
        text   = line.get("text", "")
        tc_in  = line.get("tc_in", "")
        tc_out = line.get("tc_out", "")
        dur    = line.get("dur", "")
        speaker= line.get("speaker")

        if ltype == "section":
            row = table.add_row()
            row.cells[0].merge(row.cells[3])
            c = row.cells[0]; set_bg(c, "1F3864"); set_borders(c, "1F3864")
            c.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            p = c.paragraphs[0]; p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run(p, text.upper(), bold=True, color="#FFFFFF", size=10)
            continue

        if ltype == "part":
            row = table.add_row()
            row.cells[0].merge(row.cells[3])
            c = row.cells[0]; set_bg(c, "2E5DA0"); set_borders(c, "2E5DA0")
            c.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            p = c.paragraphs[0]; p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run(p, text, bold=True, color="#FFFFFF", size=9)
            continue

        if ltype == "coda":
            row = table.add_row()
            row.cells[0].merge(row.cells[3])
            c = row.cells[0]; set_bg(c, "E2EFDA"); set_borders(c, "AAAAAA")
            p = c.paragraphs[0]
            for part in text.split(" / "):
                run(p, part.strip() + "\n", size=9)
            continue

        row = table.add_row()
        for cell, val in zip(row.cells[:3], [tc_in, tc_out, dur]):
            set_borders(cell)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.TOP
            run(cell.paragraphs[0], val, mono=True, size=8)

        sc = row.cells[3]; set_borders(sc)
        sc.vertical_alignment = WD_ALIGN_VERTICAL.TOP

        if ltype == "vo":
            p = sc.paragraphs[0]
            run(p, "VO:  ", bold=True, size=9)
            for part in text.split(" / "):
                run(p, part.strip() + "  ", bold=True, size=9)
        else:
            set_bg(sc, "F5F5F5")
            run(sc.paragraphs[0], "Actuality:", italic=True, size=8, color="#666666")
            if speaker:
                run(sc.add_paragraph(), speaker, bold=True, size=9)
            run(sc.add_paragraph(), text, italic=True, size=9)

    doc.save(str(output_path))


# ─────────────────────────────────────────────────────────────────────────────
# Background job runner
# ─────────────────────────────────────────────────────────────────────────────

def run_job(job_id, script_path, video_path, tc_offset, fps, api_key, output_path):
    job = jobs[job_id]

    def log(msg, pct=None):
        job["log"].append(msg)
        if pct is not None:
            job["progress"] = pct
        print(f"[{job_id[:8]}] {msg}")

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        # 1 — Parse script
        log("Parsing source script…", 5)
        script_lines = parse_source_script(script_path)
        log(f"✓ Found {len(script_lines)} lines in script.", 15)

        # 2 — Audio: extract from video, or use directly if already audio
        AUDIO_EXTS = {".mp3", ".mp4a", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus"}
        VIDEO_EXTS = {".mp4", ".mov", ".mxf", ".avi", ".mkv", ".mts", ".m2ts"}
        upload_ext = Path(video_path).suffix.lower()

        mp3 = tmp_dir / "audio.mp3"

        if upload_ext in AUDIO_EXTS and upload_ext == ".mp3":
            # Already an MP3 — check size and use directly
            size_mb = Path(video_path).stat().st_size / 1024 / 1024
            log(f"Audio file detected ({size_mb:.1f} MB) — skipping extraction…", 20)
            if size_mb > 24:
                # Still too big — re-compress at lower bitrate
                log("File over 25 MB — re-compressing…", 25)
                compress_audio(video_path, mp3)
            else:
                import shutil as _sh
                _sh.copy2(video_path, mp3)
            size_mb = mp3.stat().st_size / 1024 / 1024
            log(f"✓ Audio ready — {size_mb:.1f} MB", 35)

        elif upload_ext in AUDIO_EXTS:
            # Other audio format — just compress/convert to MP3
            size_mb = Path(video_path).stat().st_size / 1024 / 1024
            log(f"Audio file detected ({upload_ext}, {size_mb:.1f} MB) — converting to MP3…", 20)
            compress_audio(video_path, mp3)
            size_mb = mp3.stat().st_size / 1024 / 1024
            log(f"✓ Audio ready — {size_mb:.1f} MB", 35)

        else:
            # Video file — extract then compress as before
            log("Extracting audio from video (ffmpeg)…", 20)
            wav = tmp_dir / "audio.wav"
            extract_audio(video_path, wav)
            log("Compressing audio for upload…", 30)
            compress_audio(wav, mp3)
            size_mb = mp3.stat().st_size / 1024 / 1024
            log(f"✓ Audio ready — {size_mb:.1f} MB", 35)

        # 3 — Transcribe
        log("Sending to OpenAI Whisper API…", 40)
        segments = transcribe_openai(mp3, api_key)
        log(f"✓ Transcription complete — {len(segments)} segments", 70)

        # 4 — Match
        log("Matching timecodes to script…", 75)
        offset  = tc_to_seconds(tc_offset)
        matched = match_timecodes(script_lines, segments, offset, fps)
        n_matched = sum(1 for m in matched if m["tc_in"])
        log(f"✓ Matched {n_matched} / {len(matched)} lines", 88)

        # 5 — Build docx
        log("Building output document…", 92)
        build_output_docx(matched, output_path, fps)
        log(f"✓ Done!", 100)

        job["status"]      = "done"
        job["output_path"] = str(output_path)

    except Exception as e:
        job["status"] = "error"
        job["error"]  = str(e)
        log(f"✗ Error: {e}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        # Clean up uploaded files
        try: os.remove(script_path)
        except: pass
        try: os.remove(video_path)
        except: pass


# ─────────────────────────────────────────────────────────────────────────────
# Flask routes
# ─────────────────────────────────────────────────────────────────────────────

UPLOAD_DIR = Path(tempfile.gettempdir()) / "pp_timecoder_uploads"
OUTPUT_DIR = Path(tempfile.gettempdir()) / "pp_timecoder_outputs"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/start", methods=["POST"])
def start_job():
    # Validate files
    if "script" not in request.files or "video" not in request.files:
        return jsonify({"error": "Both script and video files are required."}), 400

    script_file = request.files["script"]
    video_file  = request.files["video"]
    api_key     = request.form.get("api_key", "").strip()
    tc_offset   = request.form.get("tc_offset", "10:00:00:00").strip()
    fps         = float(request.form.get("fps", "25"))

    if not api_key:
        return jsonify({"error": "OpenAI API key is required."}), 400

    if not api_key.startswith("sk-"):
        return jsonify({"error": "That doesn't look like a valid OpenAI API key (should start with sk-)."}), 400

    # Save uploaded files
    job_id = str(uuid.uuid4())
    script_path = UPLOAD_DIR / f"{job_id}_script.docx"
    video_path  = UPLOAD_DIR / f"{job_id}_video{Path(video_file.filename).suffix}"
    output_path = OUTPUT_DIR / f"{job_id}_timecoded.docx"

    script_file.save(str(script_path))
    video_file.save(str(video_path))

    # Init job
    jobs[job_id] = {
        "status":      "running",
        "progress":    0,
        "log":         [],
        "output_path": None,
        "error":       None,
    }

    # Run in background
    thread = threading.Thread(
        target=run_job,
        args=(job_id, str(script_path), str(video_path),
              tc_offset, fps, api_key, output_path),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status":   job["status"],
        "progress": job["progress"],
        "log":      job["log"],
        "error":    job.get("error"),
    })


@app.route("/api/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Not ready"}), 404
    path = job["output_path"]
    if not path or not Path(path).exists():
        return jsonify({"error": "Output file missing"}), 500
    return send_file(path, as_attachment=True,
                     download_name="timecoded_script.docx")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

