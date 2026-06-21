# Carry Backend

The Carry backend powers the wearable, voice-first co-pilot for professionals. It receives audio/transcript streams from the mobile app, coordinates transcription, memory, notes, follow-up actions, and live transcript fan-out for web UIs or separate agent backends.

Carry is profession-agnostic at its core. Doctor Mode and Lawyer Mode are profession packs layered on top of the same backend workflow.

## What this backend handles

- Mobile app API endpoints.
- Firebase-backed user identity for the main app flow.
- Streaming transcription provider selection.
- Conversation storage and memory.
- Translation with fail-open behavior when translation credentials or APIs are unavailable.
- Redis-backed live transcript streams.
- Drafted notes and follow-up work that require professional review.

## Safety boundary

Carry drafts work product. It does not make final professional decisions.

- Doctor outputs require clinician review.
- Lawyer outputs require attorney review.
- No automatic diagnosis, prescription, legal advice, filing, or client/patient communication.

## Requirements

- Python 3.11
- Redis
- FFmpeg
- Native Opus library for audio streaming
- Firebase project and service account credentials
- At least one STT provider key, usually Deepgram for the default streaming flow
- Optional ngrok tunnel for testing with a physical Android device

## Environment setup

From this directory:

~~~powershell
cd C:\Aiboomi-App\carry-app\backend
copy .env.template .env
~~~

Then edit .env with your local values.

Important local values:

~~~env
REDIS_DB_HOST=localhost
REDIS_DB_PORT=6379

GOOGLE_APPLICATION_CREDENTIALS=google-credentials.json

STT_STREAMING_PROVIDER=deepgram
DEEPGRAM_API_KEY=your_deepgram_key

OPENAI_API_KEY=your_openai_key_if_using_openai_features
ADMIN_KEY=local-dev-admin-key
~~~

Do not commit real secrets.

## Firebase credentials

Place your Firebase service account JSON at:

~~~text
backend/google-credentials.json
~~~

Your Flutter app Firebase config and backend service account must point to the same Firebase project. If they do not match, Firebase ID token validation will fail with an audience/project mismatch.

## Install dependencies

Recommended local virtual environment:

~~~powershell
cd C:\Aiboomi-App\carry-app\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
~~~

If your shell blocks script activation, run PowerShell as your user and use:

~~~powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
~~~

## Redis

For local POC work, Redis can run in Docker:

~~~powershell
docker run --name carry-redis -p 6379:6379 -d redis:7
~~~

If the container already exists:

~~~powershell
docker start carry-redis
~~~

## Native Opus on Windows

Audio streaming requires the native Opus library.

One Windows option is MSYS2 UCRT64:

~~~powershell
pacman -S mingw-w64-ucrt-x86_64-opus
~~~

Then add this directory to your Windows PATH:

~~~text
C:\msys64\ucrt64\bin
~~~

Open a new terminal and verify:

~~~powershell
where.exe opus.dll
~~~

## Run the backend

~~~powershell
cd C:\Aiboomi-App\carry-app\backend
.\.venv\Scripts\python.exe -m uvicorn main:app --reload --host 0.0.0.0 --port 8000 --env-file .env
~~~

Local API:

~~~text
http://localhost:8000
~~~

## Expose with ngrok for a physical phone

~~~powershell
ngrok http 8000
~~~

Use the generated HTTPS URL in the Flutter app environment.

Example:

~~~env
BASE_API_URL=https://your-ngrok-url.ngrok-free.app
~~~

Websockets should use the same host with wss://.

## STT provider selection

Default:

~~~env
STT_STREAMING_PROVIDER=deepgram
~~~

Optional OpenAI diarized transcription flow:

~~~env
STT_STREAMING_PROVIDER=openai_diarize
OPENAI_API_KEY=your_openai_key
OPENAI_DIARIZE_MODEL=gpt-4o-transcribe-diarize
OPENAI_DIARIZE_WINDOW_SECONDS=12
OPENAI_DIARIZE_OVERLAP_SECONDS=0
OPENAI_DIARIZE_SAMPLE_RATE=16000
OPENAI_DIARIZE_CHUNKING_STRATEGY=auto
OPENAI_DIARIZE_TIMEOUT_SECONDS=60
~~~

Deepgram remains the default because it is better suited for low-latency streaming. OpenAI diarized transcription is chunk/window based and may feel slower for real-time UI.

## Live transcript websocket

Carry mirrors transcript events into Redis Streams and exposes them over websocket.

POC global stream:

~~~text
wss://<backend-host>/v4/live/transcripts
~~~

Example with ngrok:

~~~text
wss://your-ngrok-url.ngrok-free.app/v4/live/transcripts
~~~

Events include a UTC ISO timestamp:

~~~json
{
  "id": "1718870000000-0",
  "type": "transcript.updated",
  "conversation_id": "conversation-id",
  "created_at": "1718870000.123456",
  "timestamp": "2026-06-21T08:30:00.123456Z",
  "segments": []
}
~~~

The current POC global stream is intentionally no-auth and single-user friendly. Do not expose it as-is in production.

## Consume live transcript events

From the repo root:

~~~powershell
cd C:\Aiboomi-App\carry-app
$env:CARRY_BACKEND_WS_URL="https://your-ngrok-url.ngrok-free.app"
python .\example_usage\consume_live_transcript.py
~~~

If your Windows launcher is py:

~~~powershell
py .\example_usage\consume_live_transcript.py
~~~

## Discard live transcript queue

Clear the current POC live transcript Redis queue:

~~~powershell
Invoke-RestMethod -Method Post -Uri "https://your-ngrok-url.ngrok-free.app/v4/live/transcripts/discard"
~~~

Clear a specific conversation:

~~~powershell
Invoke-RestMethod -Method Post -Uri "https://your-ngrok-url.ngrok-free.app/v4/live/transcripts/YOUR_CONVERSATION_ID/discard"
~~~

The mobile app also includes a discard action from the transcript view.

## Translation behavior

If Google Cloud Translation is not configured or the API is disabled, Carry should warn and continue with untranslated text instead of breaking transcription.

To use Cloud Translation, enable the Cloud Translation API in the same Google project used by the backend credentials.

## Common issues

### Redis connection refused

Start Redis:

~~~powershell
docker start carry-redis
~~~

Or create it:

~~~powershell
docker run --name carry-redis -p 6379:6379 -d redis:7
~~~

### Firebase token audience mismatch

The Flutter app Firebase config and backend service account are from different Firebase projects. Regenerate app Firebase config or use the matching backend service account.

### Missing Opus library

Install native Opus and ensure its DLL directory is on PATH.

### Google sign-in ApiException 10

The Android package name and SHA-1/SHA-256 signing fingerprints must match the OAuth client configured in Firebase/Google Cloud.
