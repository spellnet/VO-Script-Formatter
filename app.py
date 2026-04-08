"""
PP Script Timecoder — Web App
Flask backend: upload script + video → timecoded .docx via OpenAI Whisper API
"""

import os, re, uuid, json, time, shutil, tempfile, threading, subprocess, csv
from datetime import datetime, timezone
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
    """HH:MM:SS:FF — full timecode with frames, floored so TC is never late."""
    total = max(0.0, secs + offset_secs)
    h  = int(total // 3600)
    m  = int((total % 3600) // 60)
    s  = int(total % 60)
    ff = int((total - int(total)) * fps)   # floor frames, never late
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
    """HH:MM:SS:FF — full duration with frames, floored."""
    if secs <= 0:
        return ""
    h  = int(secs // 3600)
    m  = int((secs % 3600) // 60)
    s  = int(secs % 60)
    ff = int((secs - int(secs)) * 25)
    if ff >= 25:
        ff = 24
    return f"{h:02d}:{m:02d}:{s:02d}:{ff:02d}"


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

    # NOTE: we do NOT parse doc.paragraphs (outside the table).
    # Those contain the document title block (production number, title,
    # version, date etc.) which must not be treated as script content.
    # All actual VO and actuality content lives inside the table.

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
    """
    Forward-pass fuzzy matching of script lines to Whisper segments.

    Key fixes vs previous version:
    - Cursor advances PAST the matched segment after each hit, so the same
      segment can never be reused for the next line (fixes "all same TC" bug)
    - Lower threshold (0.18) so more VO lines get matched even when Whisper
      picks up narration imperfectly through music beds
    - Wider lookahead (80 segs) to handle longer gaps between VO lines
    - Falls back to best available match if nothing meets threshold, provided
      the score is at least 0.10 -- leaves blank only if truly no signal at all
    """
    if not segments:
        return [{**l, "tc_in": "", "tc_out": "", "dur": ""} for l in script_lines]

    results    = []
    seg_cursor = 0
    WINDOW     = 10   # max consecutive segments to join when matching
    LOOKAHEAD  = 80   # how far ahead to search from current cursor
    THRESHOLD  = 0.18 # accept match above this score
    FALLBACK   = 0.10 # use best-effort match above this if nothing hits threshold
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
                joined      = (joined + " " + segments[j]["text"]).strip()
                norm_joined = normalize(joined)
                score       = similarity(norm_line, norm_joined)
                # Boost score if script text is a substring of the transcript
                if len(norm_line) > 8 and norm_line in norm_joined:
                    score = max(score, 0.75)
                if score > best_score:
                    best_score = score
                    best_i, best_j = i, j

        if best_score >= THRESHOLD and best_i is not None:
            # Advance cursor PAST the matched segment so it cannot repeat
            seg_cursor = best_j + 1
            t_in  = segments[best_i]["start"]
            t_out = segments[best_j]["end"]
            results.append({
                **line,
                "tc_in":  seconds_to_tc(t_in,  tc_offset_secs, fps),
                "tc_out": seconds_to_tc(t_out, tc_offset_secs, fps),
                "dur":    dur_str(t_out - t_in),
            })
        elif best_score >= FALLBACK and best_i is not None:
            # Best-effort: use the match but flag it with a ~ prefix so the
            # editor knows it needs checking
            seg_cursor = best_j + 1
            t_in  = segments[best_i]["start"]
            t_out = segments[best_j]["end"]
            results.append({
                **line,
                "tc_in":  "~" + seconds_to_tc(t_in,  tc_offset_secs, fps),
                "tc_out": "~" + seconds_to_tc(t_out, tc_offset_secs, fps),
                "dur":    dur_str(t_out - t_in),
            })
        else:
            results.append({**line, "tc_in": "", "tc_out": "", "dur": ""})

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Output .docx builder
# ─────────────────────────────────────────────────────────────────────────────

def build_output_docx(matched_lines, output_path, fps):
    """
    Build the VO recording script .docx matching the Stampede template exactly:
    - Calibri 10pt throughout
    - A4 page, 1-inch margins
    - Column widths: 990 / 990 / 973 / 6241 DXA
    - Story headers:  #DEEBF6 blue,  merged full width
    - TITLES/PART BREAK: #E2EFD9 green, merged
    - ACT rows:       #FBE5D5 peach, merged
    - Actuality rows: #E7E6E6 grey, TC cells blank
    - VO rows:        white,  TC cells filled
    """
    from docx import Document
    from docx.shared import Pt, RGBColor, Inches, Cm
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_ALIGN_VERTICAL

    # ── Column widths (DXA) ──────────────────────────────────────────────────
    # Widths sized for HH:MM:SS:FF (11 chars Courier New 10pt) + cell padding
    # Total kept at 9194 DXA to match original template table width
    W_TC1 = 1440   # 1 inch — fits HH:MM:SS:FF without wrapping
    W_TC2 = 1440   # 1 inch
    W_DUR = 1440   # 1 inch
    W_SCR = 4874   # remaining — ~3.4 inches for script text

    doc     = Document()
    sec     = doc.sections[0]
    sec.page_width    = Inches(8.27)   # A4
    sec.page_height   = Inches(11.69)  # A4
    sec.left_margin   = Inches(1.0)
    sec.right_margin  = Inches(1.0)
    sec.top_margin    = Inches(1.0)
    sec.bottom_margin = Inches(1.0)

    # ── Helpers ──────────────────────────────────────────────────────────────

    def set_cell_width(cell, dxa):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        # Remove existing width if present
        for old in tcPr.findall(qn("w:tcW")):
            tcPr.remove(old)
        tcW = OxmlElement("w:tcW")
        tcW.set(qn("w:w"),    str(dxa))
        tcW.set(qn("w:type"), "dxa")
        tcPr.append(tcW)

    def set_bg(cell, hex_color):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd  = OxmlElement("w:shd")
        shd.set(qn("w:val"),   "clear")
        shd.set(qn("w:color"), "auto")
        shd.set(qn("w:fill"),  hex_color.upper().replace("#", ""))
        tcPr.append(shd)

    def set_borders(cell, color="AAAAAA"):
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

    def add_run(para, text, bold=False, italic=False,
                color=None, size=10, mono=False):
        """Add a run. Font: Calibri 10pt by default."""
        r = para.add_run(text)
        r.bold      = bold
        r.italic    = italic
        r.font.size = Pt(size)
        r.font.name = "Courier New" if mono else "Calibri"
        if color:
            rgb = tuple(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
            r.font.color.rgb = RGBColor(*rgb)

    def set_row_widths(row, widths):
        for cell, w in zip(row.cells, widths):
            set_cell_width(cell, w)

    COL_WIDTHS = [W_TC1, W_TC2, W_DUR, W_SCR]

    # ── Title block (above table) ─────────────────────────────────────────────
    tp = doc.add_paragraph()
    add_run(tp, "STAMPEDE PRODUCTIONS", bold=True, size=12)
    tp2 = doc.add_paragraph()
    add_run(tp2, "VO SCRIPT  —  AUTO FORMATTED", bold=True, size=10)
    tp3 = doc.add_paragraph()
    add_run(tp3,
        f"TC: HH:MM:SS:FF  ·  {fps}fps  ·  "
        "Actuality rows shown for reference only — timecodes on VO lines",
        italic=True, size=9, color="#888888")
    doc.add_paragraph()

    # ── Table ────────────────────────────────────────────────────────────────
    table = doc.add_table(rows=0, cols=4)
    table.style = "Table Grid"

    # ── Column header row ────────────────────────────────────────────────────
    hrow = table.add_row()
    set_row_widths(hrow, COL_WIDTHS)
    for cell, label in zip(hrow.cells,
                           ["Time Code In", "Time Code Out",
                            "Duration", "SCRIPT & VO"]):
        set_borders(cell)
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        add_run(p, label, bold=True, size=10)

    # ── Helper: merged full-width label row ───────────────────────────────────
    def add_label_row(text, fill_hex, bold=False, italic=False, color=None):
        row = table.add_row()
        set_row_widths(row, COL_WIDTHS)
        row.cells[0].merge(row.cells[3])
        c = row.cells[0]
        set_bg(c, fill_hex)
        set_borders(c, "AAAAAA")
        c.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        p = c.paragraphs[0]
        add_run(p, text, bold=bold, italic=italic,
                color=color, size=10)

    # ── Classify label row colour by content ──────────────────────────────────
    import re as _re
    def label_fill(text):
        t = text.upper().strip()
        if _re.match('^ACT\\s+\\d+', t):
            return "FBE5D5"   # peach — ACT 2, ACT 3...
        if any(kw in t for kw in ["PART BREAK", "TITLES", "COLD OPEN",
                                   "TEASER", "TAG", "END OF SHOW"]):
            return "E2EFD9"   # green
        return "DEEBF6"       # blue — story segment name (default)

    # ── Data rows ────────────────────────────────────────────────────────────
    for line in matched_lines:
        ltype   = line.get("type",    "act")
        text    = line.get("text",    "")
        tc_in   = line.get("tc_in",   "")
        tc_out  = line.get("tc_out",  "")
        dur     = line.get("dur",     "")
        speaker = line.get("speaker")

        # ── Section / part / label rows ───────────────────────────────────
        if ltype in ("section", "part"):
            add_label_row(text, label_fill(text))
            continue

        # ── Coda card ─────────────────────────────────────────────────────
        if ltype == "coda":
            add_label_row(f"[CODA]  {text}", "E2EFD9", italic=True, color="#555555")
            continue

        # ── VO row ────────────────────────────────────────────────────────
        if ltype == "vo":
            row = table.add_row()
            set_row_widths(row, COL_WIDTHS)

            for cell, val in zip(row.cells[:3], [tc_in, tc_out, dur]):
                set_borders(cell)
                cell.vertical_alignment = WD_ALIGN_VERTICAL.TOP
                p = cell.paragraphs[0]
                add_run(p, val, mono=True, size=10)

            sc = row.cells[3]
            set_borders(sc)
            sc.vertical_alignment = WD_ALIGN_VERTICAL.TOP
            p = sc.paragraphs[0]
            add_run(p, "VO:", bold=True, size=10)
            # VO text on next paragraph, bold uppercase
            p2 = sc.add_paragraph()
            for part in text.split(" / "):
                add_run(p2, part.strip() + " ", bold=True, size=10)
            continue

        # ── Actuality row — TC cells BLANK, grey background ───────────────
        row = table.add_row()
        set_row_widths(row, COL_WIDTHS)

        for cell in row.cells[:4]:   # grey ALL cells including script col
            set_bg(cell, "E7E6E6")
            set_borders(cell)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.TOP

        # TC cells explicitly empty
        for cell in row.cells[:3]:
            cell.paragraphs[0].clear()

        sc = row.cells[3]
        p = sc.paragraphs[0]
        add_run(p, "Actuality:", italic=True, size=10)

        if speaker:
            p2 = sc.add_paragraph()
            add_run(p2, speaker, bold=True, size=10)

        p3 = sc.add_paragraph()
        for part in text.split(" / "):
            add_run(p3, part.strip() + " ", italic=True, size=10)

    doc.save(str(output_path))


# ─────────────────────────────────────────────────────────────────────────────
# Email notifications via Resend
# ─────────────────────────────────────────────────────────────────────────────

def send_notify_email(user_name, script_name, video_name,
                      n_matched, n_total, status, error):
    """
    Send a job completion/failure email via Resend API.
    Requires env vars:
      RESEND_API_KEY  — from resend.com (free tier: 3000 emails/month)
      NOTIFY_EMAIL    — address to send notifications TO
      NOTIFY_FROM     — sending address (must be verified in Resend)
                        e.g. "Stampede Formatter <notifications@yourdomain.com>"
                        or use Resend's default: "onboarding@resend.dev" for testing
    """
    import urllib.request, urllib.error, json as _json

    api_key    = os.environ.get("RESEND_API_KEY", "").strip()
    to_email   = os.environ.get("NOTIFY_EMAIL",   "").strip()
    from_addr  = os.environ.get("NOTIFY_FROM",    "Stampede VO Formatter <onboarding@resend.dev>").strip()

    if not api_key or not to_email:
        return  # Silently skip if not configured

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if status == "done":
        subject = f"✓ VO Script ready — {user_name} — {script_name}"
        body = f"""<html><body style="font-family:Arial,sans-serif;color:#222;padding:24px">
<h2 style="color:#1F3864">Stampede VO Script Auto Formatter</h2>
<p style="font-size:15px">A new script has been formatted successfully.</p>
<table style="border-collapse:collapse;margin:20px 0;font-size:14px">
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">User</td>
      <td style="padding:6px 0">{user_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Script</td>
      <td style="padding:6px 0">{script_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Video</td>
      <td style="padding:6px 0">{video_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Lines matched</td>
      <td style="padding:6px 0">{n_matched} / {n_total}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Time</td>
      <td style="padding:6px 0">{now}</td></tr>
</table>
<p style="color:#888;font-size:12px">Stampede Productions · VO Script Auto Formatter</p>
</body></html>"""
    else:
        subject = f"✗ VO Formatter error — {user_name} — {script_name}"
        body = f"""<html><body style="font-family:Arial,sans-serif;color:#222;padding:24px">
<h2 style="color:#c0392b">Stampede VO Formatter — Job Failed</h2>
<table style="border-collapse:collapse;margin:20px 0;font-size:14px">
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">User</td>
      <td>{user_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Script</td>
      <td>{script_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Video</td>
      <td>{video_name}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Error</td>
      <td style="color:#c0392b">{error}</td></tr>
  <tr><td style="padding:6px 16px 6px 0;color:#666;font-weight:bold">Time</td>
      <td>{now}</td></tr>
</table>
</body></html>"""

    payload = _json.dumps({
        "from":    from_addr,
        "to":      [to_email],
        "subject": subject,
        "html":    body,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data    = payload,
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method = "POST"
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"Email send failed (non-fatal): {e}")


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

        # Send completion email
        send_notify_email(
            user_name    = job.get("user_name", "unknown"),
            script_name  = job.get("script_name", ""),
            video_name   = job.get("video_name", ""),
            n_matched    = n_matched,
            n_total      = len(matched),
            status       = "done",
            error        = None,
        )

    except Exception as e:
        job["status"] = "error"
        job["error"]  = str(e)
        log(f"✗ Error: {e}")
        send_notify_email(
            user_name   = job.get("user_name", "unknown"),
            script_name = job.get("script_name", ""),
            video_name  = job.get("video_name", ""),
            n_matched   = 0,
            n_total     = 0,
            status      = "error",
            error       = str(e),
        )
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
LOG_FILE   = Path(tempfile.gettempdir()) / "pp_timecoder_usage.csv"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

PERMANENT_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()


def write_usage_log(user_name, script_filename, video_filename, status, note=""):
    is_new = not LOG_FILE.exists()
    try:
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(["timestamp_utc", "user_name", "script_file", "video_file", "status", "note"])
            w.writerow([
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                user_name or "unknown",
                script_filename, video_filename, status, note,
            ])
    except Exception as e:
        print(f"Usage log write failed: {e}")


@app.route("/")
def index():
    return render_template("index.html", has_permanent_key=bool(PERMANENT_API_KEY))


@app.route("/api/start", methods=["POST"])
def start_job():
    if "script" not in request.files or "video" not in request.files:
        return jsonify({"error": "Both script and video files are required."}), 400

    script_file = request.files["script"]
    video_file  = request.files["video"]
    user_name   = request.form.get("user_name", "").strip()
    tc_offset   = request.form.get("tc_offset", "10:00:00:00").strip()
    fps         = float(request.form.get("fps", "25"))

    if PERMANENT_API_KEY:
        api_key = PERMANENT_API_KEY
    else:
        api_key = request.form.get("api_key", "").strip()
        if not api_key:
            return jsonify({"error": "OpenAI API key is required."}), 400
        if not api_key.startswith("sk-"):
            return jsonify({"error": "That does not look like a valid OpenAI API key."}), 400

    if not user_name:
        return jsonify({"error": "Please enter your name so we can track usage."}), 400

    job_id      = str(uuid.uuid4())
    script_path = UPLOAD_DIR / f"{job_id}_script.docx"
    video_path  = UPLOAD_DIR / f"{job_id}_video{Path(video_file.filename).suffix}"
    output_path = OUTPUT_DIR / f"{job_id}_timecoded.docx"

    script_file.save(str(script_path))
    video_file.save(str(video_path))
    write_usage_log(user_name, script_file.filename, video_file.filename, "started")

    jobs[job_id] = {
        "status":      "running",
        "progress":    0,
        "log":         [],
        "output_path": None,
        "error":       None,
        "user_name":   user_name,
        "script_name": script_file.filename,
        "video_name":  video_file.filename,
    }

    thread = threading.Thread(
        target=run_job,
        args=(job_id, str(script_path), str(video_path), tc_offset, fps, api_key, output_path),
        daemon=True
    )
    thread.start()
    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job["status"] in ("done", "error") and not job.get("_logged"):
        job["_logged"] = True
        write_usage_log(
            job.get("user_name"), job.get("script_name"), job.get("video_name"),
            job["status"],
            job.get("error", "")[:200] if job["status"] == "error" else ""
        )
    return jsonify({"status": job["status"], "progress": job["progress"],
                    "log": job["log"], "error": job.get("error")})


@app.route("/api/download/<job_id>")
def download(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Not ready"}), 404
    path = job["output_path"]
    if not path or not Path(path).exists():
        return jsonify({"error": "Output file missing"}), 500
    return send_file(path, as_attachment=True, download_name="timecoded_script.docx")


@app.route("/usage")
def usage_log():
    pw = os.environ.get("USAGE_PASSWORD", "")
    if pw and request.args.get("key") != pw:
        return "Unauthorised", 403
    if not LOG_FILE.exists():
        return "<p>No usage data yet.</p>"
    with open(LOG_FILE, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if len(rows) < 2:
        return "<p>No usage data yet.</p>"
    headers, data = rows[0], list(reversed(rows[1:]))
    tbl = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in data)
    hdr = "".join(f"<th>{h}</th>" for h in headers)
    return f"""<!DOCTYPE html><html><head><title>Usage Log</title>
<style>body{{font-family:Arial,sans-serif;padding:30px;background:#0f0f1a;color:#e8e8f0}}
h1{{color:#5b6ef5;margin-bottom:20px}}table{{border-collapse:collapse;width:100%;font-size:13px}}
th{{background:#1f3864;color:#fff;padding:10px 14px;text-align:left}}
td{{padding:8px 14px;border-bottom:1px solid #2e2e50}}tr:hover td{{background:#1a1a2e}}</style>
</head><body><h1>Stampede VO Formatter — Usage Log</h1>
<p style=color:#8080a8>{len(data)} jobs total</p>
<table><thead><tr>{hdr}</tr></thead><tbody>{tbl}</tbody></table></body></html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
