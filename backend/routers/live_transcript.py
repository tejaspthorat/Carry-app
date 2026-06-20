import asyncio
import hmac
import json
import logging
import os
import time
from typing import Optional

from fastapi import APIRouter
from fastapi.websockets import WebSocket, WebSocketDisconnect
from firebase_admin.auth import InvalidIdTokenError

import database.conversations as conversations_db
from utils.executors import db_executor, run_blocking
from utils.live_transcript_stream import (
    get_latest_live_transcript_event_id_async,
    get_latest_live_transcript_global_event_id_async,
    get_live_transcript_uid_for_conversation_async,
    normalize_stream_id,
    read_live_transcript_global_events_async,
    read_live_transcript_events_async,
)
from utils.other import endpoints as auth

logger = logging.getLogger(__name__)

router = APIRouter()


def _auth_payload_from_first_message(message: dict) -> dict:
    """Parse the same first-message auth envelope used by /v4/web/listen."""

    text = message.get('text')
    if text is None:
        raise ValueError('Expected JSON auth message')
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError('Invalid JSON') from e
    if payload.get('type') != 'auth':
        raise ValueError('First message must be auth')
    return payload


def _uid_from_shared_password(payload: dict) -> Optional[str]:
    """Return uid when LIVE_TRANSCRIPT_PASSWORD auth is used.

    This is intentionally simple for local/dev agent integrations. It should
    not be treated as a replacement for Firebase auth in production.
    """

    if 'password' not in payload:
        return None

    expected_password = os.getenv('LIVE_TRANSCRIPT_PASSWORD')
    if not expected_password:
        raise ValueError('LIVE_TRANSCRIPT_PASSWORD is not configured')

    provided_password = str(payload.get('password') or '')
    if not hmac.compare_digest(provided_password, expected_password):
        raise ValueError('Invalid live transcript password')

    return str(payload.get('uid') or '').strip() or None


@router.websocket('/v4/live/transcripts')
async def live_transcript_global_events_handler(websocket: WebSocket, last_event_id: Optional[str] = None):
    """POC-only global live transcript stream.

    No auth and no conversation id. This is intentionally simple for a local
    single-user proof of concept. Do not expose this endpoint in production.
    """

    try:
        await websocket.accept()
    except RuntimeError as e:
        logger.error('live_transcript_global_events_handler: accept error %s', e)
        return

    cursor = normalize_stream_id(last_event_id or '0-0')
    if cursor == '$':
        try:
            cursor = await get_latest_live_transcript_global_event_id_async()
        except Exception as e:
            logger.error('global live transcript latest cursor lookup failed: %s', e)
            await websocket.close(code=1011, reason='Redis stream unavailable')
            return

    await websocket.send_json(
        {
            'type': 'live_transcript_connected',
            'stream': 'redis',
            'scope': 'global',
            'last_event_id': cursor,
        }
    )

    logger.warning('POC global live transcript stream connected with no auth')
    last_heartbeat = time.time()

    try:
        while True:
            events = await read_live_transcript_global_events_async(cursor)
            if events:
                for event in events:
                    await websocket.send_json(event)
                    cursor = event['id']
                continue

            now = time.time()
            if now - last_heartbeat >= 15:
                await websocket.send_json(
                    {
                        'type': 'live_transcript_heartbeat',
                        'scope': 'global',
                        'last_event_id': cursor,
                        'created_at': str(now),
                    }
                )
                last_heartbeat = now
    except WebSocketDisconnect:
        logger.info('global live transcript stream disconnected')
    except Exception as e:
        logger.error('global live transcript stream error: %s', e)
        try:
            await websocket.send_json({'type': 'live_transcript_error', 'message': 'Redis stream unavailable'})
            await websocket.close(code=1011, reason='Redis stream unavailable')
        except Exception:
            pass


@router.websocket('/v4/live/transcripts/{conversation_id}')
async def live_transcript_events_handler(
    websocket: WebSocket,
    conversation_id: str,
    last_event_id: Optional[str] = None,
    include_snapshot: bool = True,
):
    """Replay and stream live transcript events for a web UI or agent backend.

    First client message:
        {"type": "auth", "token": "<firebase_id_token>", "last_event_id": "0-0"}

    Local/dev password auth is also supported when LIVE_TRANSCRIPT_PASSWORD is
    set:
        {"type": "auth", "password": "<shared_password>", "last_event_id": "0-0"}

    If the Redis conversation-id lookup has expired, include uid as a fallback:
        {"type": "auth", "uid": "<uid>", "password": "<shared_password>", "last_event_id": "0-0"}

    The last_event_id may also be passed as a query parameter. Use "latest" to
    skip retained history and receive only events created after connection.
    """

    try:
        await websocket.accept()
    except RuntimeError as e:
        logger.error('live_transcript_events_handler: accept error %s', e)
        return

    try:
        first_message = await asyncio.wait_for(websocket.receive(), timeout=5.0)
        auth_payload = _auth_payload_from_first_message(first_message)
        uid = _uid_from_shared_password(auth_payload)
        auth_mode = 'password' if 'password' in auth_payload else 'firebase'
        if auth_mode == 'password' and not uid:
            uid = await get_live_transcript_uid_for_conversation_async(conversation_id)
            if not uid:
                raise ValueError(
                    'Missing uid for password auth; Redis conversation lookup was not found. '
                    'Start a new recording after backend restart or send uid in the auth message.'
                )
        elif not uid:
            uid = auth.get_current_user_uid_from_ws_message(first_message)
    except asyncio.TimeoutError:
        await websocket.close(code=1008, reason='Auth timeout')
        return
    except WebSocketDisconnect:
        return
    except InvalidIdTokenError:
        await websocket.send_json({'type': 'auth_response', 'success': False})
        await websocket.close(code=1008, reason='Invalid token')
        return
    except ValueError as e:
        await websocket.close(code=1008, reason=str(e))
        return
    except Exception as e:
        logger.error('live_transcript_events_handler: auth error %s', e)
        await websocket.send_json({'type': 'auth_response', 'success': False})
        await websocket.close(code=1008, reason='Auth error')
        return

    requested_last_event_id = last_event_id or auth_payload.get('last_event_id') or '0-0'
    cursor = normalize_stream_id(requested_last_event_id)

    try:
        conversation = await run_blocking(db_executor, conversations_db.get_conversation, uid, conversation_id)
    except Exception as e:
        logger.error('live transcript conversation lookup failed uid=%s conversation=%s: %s', uid, conversation_id, e)
        await websocket.send_json({'type': 'auth_response', 'success': False})
        await websocket.close(code=1011, reason='Conversation lookup failed')
        return

    if not conversation:
        await websocket.send_json({'type': 'auth_response', 'success': False})
        await websocket.close(code=1008, reason='Conversation not found')
        return

    if cursor == '$':
        try:
            cursor = await get_latest_live_transcript_event_id_async(uid, conversation_id)
        except Exception as e:
            logger.error('live transcript latest cursor lookup failed uid=%s conversation=%s: %s', uid, conversation_id, e)
            await websocket.send_json({'type': 'auth_response', 'success': False})
            await websocket.close(code=1011, reason='Redis stream unavailable')
            return

    await websocket.send_json(
        {
            'type': 'auth_response',
            'success': True,
            'stream': 'redis',
            'auth_mode': auth_mode,
            'conversation_id': conversation_id,
            'last_event_id': cursor,
        }
    )

    if include_snapshot:
        await websocket.send_json(
            {
                'type': 'live_transcript_snapshot',
                'conversation_id': conversation_id,
                'segments': conversation.get('transcript_segments', []),
                'status': conversation.get('status'),
                'last_event_id': cursor,
            }
        )

    logger.info('live transcript stream connected uid=%s conversation=%s cursor=%s', uid, conversation_id, cursor)
    last_heartbeat = time.time()

    try:
        while True:
            events = await read_live_transcript_events_async(uid, conversation_id, cursor)
            if events:
                for event in events:
                    await websocket.send_json(event)
                    cursor = event['id']
                continue

            now = time.time()
            if now - last_heartbeat >= 15:
                await websocket.send_json(
                    {
                        'type': 'live_transcript_heartbeat',
                        'conversation_id': conversation_id,
                        'last_event_id': cursor,
                        'created_at': str(now),
                    }
                )
                last_heartbeat = now
    except WebSocketDisconnect:
        logger.info('live transcript stream disconnected uid=%s conversation=%s', uid, conversation_id)
    except Exception as e:
        logger.error('live transcript stream error uid=%s conversation=%s: %s', uid, conversation_id, e)
        try:
            await websocket.send_json({'type': 'live_transcript_error', 'message': 'Redis stream unavailable'})
            await websocket.close(code=1011, reason='Redis stream unavailable')
        except Exception:
            pass
