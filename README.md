# Carry

Carry is a wearable, voice-first co-pilot for professionals. It listens during conversations and carries context forward into notes, memory, and follow-up actions.

The app is a generic anchor for every profession pack. It is not only a doctor app or a lawyer app: the same capture, transcription, memory, review, and follow-up workflow can be shaped for different professional contexts.

## Profession packs

### Doctor Mode

Doctor Mode is the first profession pack. A clinician wears the capture device during a visit. Carry privacy-filters the stream, understands the conversation as it happens, drafts clinical work product at the end, remembers relevant context across sessions, and prepares follow-up work.

Doctor outputs require clinician review. Carry must not automatically diagnose, prescribe, file, message a patient, or perform clinical actions without review.

### Lawyer Mode

Lawyer Mode is an independent demo that applies the same core workflow to attorney-client meetings. It captures meeting context, prepares notes, preserves relevant memory, and drafts follow-up work for attorney review.

Lawyer outputs require attorney review. Carry must not automatically provide legal advice, file documents, communicate with clients, or take legal actions without review.

## What Carry does

- Captures voice from a wearable or mobile session.
- Streams transcription events to the backend.
- Privacy-filters and structures the conversation.
- Maintains memory across sessions.
- Drafts notes and follow-up actions.
- Sends live transcript events to web UIs and agent backends.
- Keeps professional review as the final decision point.

## Repository layout

~~~text
carry-app/
  app/              Flutter mobile app: the profession-agnostic capture anchor
  backend/          FastAPI backend: auth, transcription, memory, actions, live streams
  example_usage/    Small scripts for consuming backend APIs
~~~

## Local development

Backend setup lives in [backend/README.md](backend/README.md).

For a typical local Android + backend setup:

1. Start Redis.
2. Start the Carry backend from backend/.
3. Expose the backend with ngrok if testing on a physical phone.
4. Point the Flutter app environment to the backend URL.
5. Run the Flutter app on the connected Android device.

## Live transcript stream

Carry publishes live transcript events through Redis Streams and exposes them over websocket for a web UI or separate agent backend.

For the current local POC, the simplest stream is:

~~~text
wss://<your-backend-host>/v4/live/transcripts
~~~

Events include a UTC ISO timestamp:

~~~json
{
  "type": "transcript.updated",
  "timestamp": "2026-06-21T08:30:00.123456Z"
}
~~~

## Safety boundary

Carry is a drafting and context system. It helps professionals move faster, but review remains mandatory.

No automatic diagnosis, prescription, legal advice, filing, or client/patient communication should happen without explicit professional review.
