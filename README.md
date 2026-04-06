# PP Script Timecoder — Web App

Browser-based tool. Upload a source script (.docx) + video,
get a timecoded post-production script back. Uses OpenAI's
Whisper API for transcription — no GPU or local install needed.

---

## Deploy to Railway (free, ~5 minutes)

Railway gives you a live URL anyone can use from their browser.

### One-time setup

1. Create a free account at https://railway.app

2. Install the Railway CLI (optional — you can also use the website):
   ```
   npm install -g @railway/cli
   ```

3. Put this folder on GitHub:
   - Go to https://github.com/new and create a new repository
   - Upload these files (drag and drop works in the GitHub UI)

4. In Railway:
   - Click **New Project → Deploy from GitHub repo**
   - Select your repo
   - Railway detects the Dockerfile automatically
   - Click **Deploy**
   - In ~2 minutes you'll get a URL like `https://pp-timecoder.up.railway.app`

That's it. Share the URL with anyone.

---

## How it works

1. User opens the URL in any browser
2. Drops in their source script (.docx) and video file
3. Enters their own OpenAI API key (or you can hard-code one — see below)
4. Clicks Generate
5. Server extracts audio with ffmpeg, sends to OpenAI Whisper API,
   matches segments to script lines, builds the .docx
6. Download button appears — click to save the timecoded script

**Cost:** OpenAI Whisper API charges ~$0.006/minute of audio.
A 42-minute programme costs about $0.25 (≈£0.20).

---

## Optional: hard-code your own API key

If you want colleagues to use it without needing their own API key,
set an environment variable in Railway:

1. In your Railway project → Variables → New Variable
2. Name: `OPENAI_API_KEY`  Value: `sk-your-key-here`

Then in app.py, change the api_key line in start_job() to:
```python
api_key = os.environ.get("OPENAI_API_KEY") or request.form.get("api_key", "").strip()
```

And remove the API key field from the HTML form, or make it optional.

---

## File size limits

- Railway's free tier allows up to 1 GB uploads
- OpenAI Whisper API accepts up to 25 MB audio files
- The app auto-extracts and compresses audio (64kbps MP3) before sending
- A 42-min programme compresses to ~20 MB — safely within the limit

---

## Local development

```bash
pip install -r requirements.txt
python app.py
# Open http://localhost:5000
```

ffmpeg must be installed and on your PATH locally.

---

## Files

```
pp_web/
├── app.py              # Flask backend — all processing logic
├── templates/
│   └── index.html      # Single-page frontend
├── requirements.txt    # Python dependencies
├── Dockerfile          # Tells Railway how to build (includes ffmpeg)
└── README.md           # This file
```
