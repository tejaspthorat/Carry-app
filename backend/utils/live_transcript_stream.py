"""Durable live transcript fan-out via Redis Streams.

The mobile listen socket remains the source of truth for transcript creation.
This module mirrors transcript/translation updates into Redis so web UIs and
agent backends can reconnect, replay missed entries, then tail live updates.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from database.redis_db import r
from utils.executors import db_executor, run_blocking

logger = logging.getLogger(__name__)

EVENT_CONVERSATION_STARTED = 'conversation.started'
EVENT_TRANSCRIPT_UPDATED = 'transcript.updated'
EVENT_TRANSCRIPT_DELETED = 'transcript.deleted'
EVENT_TRANSLATION_READY = 'translation.ready'


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == '':
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning('%s=%r is not a valid integer; using %s', name, raw, default)
        return default


STREAM_PREFIX = os.getenv('LIVE_TRANSCRIPT_STREAM_PREFIX', 'live_transcript')
GLOBAL_STREAM_KEY = os.getenv('LIVE_TRANSCRIPT_GLOBAL_STREAM_KEY', 'live_transcript:global')
CONVERSATION_UID_PREFIX = os.getenv('LIVE_TRANSCRIPT_CONVERSATION_UID_PREFIX', 'live_transcript_conversation_uid')
STREAM_MAXLEN = _env_int('LIVE_TRANSCRIPT_STREAM_MAXLEN', 5000)
STREAM_TTL_SECONDS = _env_int('LIVE_TRANSCRIPT_STREAM_TTL_SECONDS', 24 * 60 * 60)
STREAM_READ_BLOCK_MS = _env_int('LIVE_TRANSCRIPT_STREAM_READ_BLOCK_MS', 5000)
STREAM_READ_COUNT = _env_int('LIVE_TRANSCRIPT_STREAM_READ_COUNT', 100)


def _safe_key_part(value: str) -> str:
    """Keep Redis keys readable enough while avoiding raw separators/user input."""

    return base64.urlsafe_b64encode(value.encode('utf-8')).decode('ascii').rstrip('=')


def _decode_key_part(value: str) -> str:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode('ascii')).decode('utf-8')


def get_live_transcript_stream_key(uid: str, conversation_id: str) -> str:
    return f'{STREAM_PREFIX}:{_safe_key_part(uid)}:{_safe_key_part(conversation_id)}'


def get_live_transcript_conversation_uid_key(conversation_id: str) -> str:
    return f'{CONVERSATION_UID_PREFIX}:{_safe_key_part(conversation_id)}'


def normalize_stream_id(last_event_id: Optional[str]) -> str:
    if not last_event_id or last_event_id == 'earliest':
        return '0-0'
    if last_event_id == 'latest':
        return '$'
    return last_event_id


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return str(value)


def _decode_fields(fields: Dict[Any, Any]) -> Dict[str, str]:
    return {_decode(k): _decode(v) for k, v in fields.items()}


def _loads_json(value: Optional[str], fallback: Any) -> Any:
    if value is None:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        logger.warning('Invalid JSON payload in live transcript stream entry: %r', value)
        return fallback


def _entry_to_event(entry_id: Any, fields: Dict[Any, Any]) -> Dict[str, Any]:
    decoded = _decode_fields(fields)
    payload = _loads_json(decoded.get('payload'), {})
    event = {
        'id': _decode(entry_id),
        'type': decoded.get('type', 'unknown'),
        'conversation_id': decoded.get('conversation_id'),
        'created_at': decoded.get('created_at'),
    }
    if decoded.get('uid'):
        event['uid'] = decoded['uid']
    if decoded.get('session_id'):
        event['session_id'] = decoded['session_id']
    if isinstance(payload, dict):
        event.update(payload)
    else:
        event['payload'] = payload
    return event


def publish_live_transcript_event(
    uid: str,
    conversation_id: str,
    event_type: str,
    payload: Dict[str, Any],
    *,
    session_id: Optional[str] = None,
) -> Optional[str]:
    """Publish one live transcript event.

    Publishing is deliberately fail-open: Redis downtime must not break the
    active phone transcription session.
    """

    if not uid or not conversation_id:
        return None

    stream_key = get_live_transcript_stream_key(uid, conversation_id)
    fields = {
        'type': event_type,
        'uid': uid,
        'conversation_id': conversation_id,
        'created_at': str(time.time()),
        'payload': json.dumps(payload or {}, default=str, ensure_ascii=False),
    }
    if session_id:
        fields['session_id'] = session_id

    try:
        entry_id = r.xadd(stream_key, fields, maxlen=STREAM_MAXLEN, approximate=True)
        r.xadd(GLOBAL_STREAM_KEY, fields, maxlen=STREAM_MAXLEN, approximate=True)
        if STREAM_TTL_SECONDS > 0:
            r.expire(stream_key, STREAM_TTL_SECONDS)
            r.expire(GLOBAL_STREAM_KEY, STREAM_TTL_SECONDS)
            r.set(get_live_transcript_conversation_uid_key(conversation_id), uid, ex=STREAM_TTL_SECONDS)
        else:
            r.set(get_live_transcript_conversation_uid_key(conversation_id), uid)
        return _decode(entry_id)
    except Exception as e:  # noqa: BLE001 - streaming is best-effort fan-out
        logger.warning(
            'Failed to publish live transcript event type=%s conversation=%s: %s',
            event_type,
            conversation_id,
            e,
        )
        return None


async def publish_live_transcript_event_async(
    uid: str,
    conversation_id: str,
    event_type: str,
    payload: Dict[str, Any],
    *,
    session_id: Optional[str] = None,
) -> Optional[str]:
    return await run_blocking(
        db_executor,
        publish_live_transcript_event,
        uid,
        conversation_id,
        event_type,
        payload,
        session_id=session_id,
    )


def get_latest_live_transcript_event_id(uid: str, conversation_id: str) -> str:
    """Return the current tail id, or 0-0 when the stream does not exist."""

    stream_key = get_live_transcript_stream_key(uid, conversation_id)
    entries = r.xrevrange(stream_key, count=1)
    if not entries:
        return '0-0'
    return _decode(entries[0][0])


async def get_latest_live_transcript_event_id_async(uid: str, conversation_id: str) -> str:
    return await run_blocking(db_executor, get_latest_live_transcript_event_id, uid, conversation_id)


def get_latest_live_transcript_global_event_id() -> str:
    entries = r.xrevrange(GLOBAL_STREAM_KEY, count=1)
    if not entries:
        return '0-0'
    return _decode(entries[0][0])


async def get_latest_live_transcript_global_event_id_async() -> str:
    return await run_blocking(db_executor, get_latest_live_transcript_global_event_id)


def get_live_transcript_uid_for_conversation(conversation_id: str) -> Optional[str]:
    mapping_key = get_live_transcript_conversation_uid_key(conversation_id)
    uid = r.get(mapping_key)
    if uid:
        return _decode(uid)

    # Backward/repair path: if the stream exists but the small lookup key was
    # not created yet (for example, backend restarted after a stream was
    # already active), recover the uid from the stream key itself.
    safe_conversation_id = _safe_key_part(conversation_id)
    prefix = f'{STREAM_PREFIX}:'
    suffix = f':{safe_conversation_id}'
    pattern = f'{prefix}*{suffix}'

    for raw_key in r.scan_iter(match=pattern, count=100):
        key = _decode(raw_key)
        if not key.startswith(prefix) or not key.endswith(suffix):
            continue
        encoded_uid = key[len(prefix) : -len(suffix)]
        try:
            uid = _decode_key_part(encoded_uid)
        except Exception:
            logger.debug('Could not decode live transcript uid from stream key: %s', key)
            continue
        if STREAM_TTL_SECONDS > 0:
            r.set(mapping_key, uid, ex=STREAM_TTL_SECONDS)
        else:
            r.set(mapping_key, uid)
        return uid

    return None


async def get_live_transcript_uid_for_conversation_async(conversation_id: str) -> Optional[str]:
    return await run_blocking(db_executor, get_live_transcript_uid_for_conversation, conversation_id)


def read_live_transcript_global_events(
    last_event_id: str,
    *,
    block_ms: int = STREAM_READ_BLOCK_MS,
    count: int = STREAM_READ_COUNT,
) -> List[Dict[str, Any]]:
    response = r.xread({GLOBAL_STREAM_KEY: last_event_id}, count=count, block=block_ms)
    events: List[Dict[str, Any]] = []
    for _, entries in response or []:
        for entry_id, fields in entries:
            events.append(_entry_to_event(entry_id, fields))
    return events


async def read_live_transcript_global_events_async(
    last_event_id: str,
    *,
    block_ms: int = STREAM_READ_BLOCK_MS,
    count: int = STREAM_READ_COUNT,
) -> List[Dict[str, Any]]:
    return await run_blocking(
        db_executor,
        read_live_transcript_global_events,
        last_event_id,
        block_ms=block_ms,
        count=count,
    )


def read_live_transcript_events(
    uid: str,
    conversation_id: str,
    last_event_id: str,
    *,
    block_ms: int = STREAM_READ_BLOCK_MS,
    count: int = STREAM_READ_COUNT,
) -> List[Dict[str, Any]]:
    """Read entries newer than last_event_id from a user's conversation stream."""

    stream_key = get_live_transcript_stream_key(uid, conversation_id)
    response = r.xread({stream_key: last_event_id}, count=count, block=block_ms)
    events: List[Dict[str, Any]] = []
    for _, entries in response or []:
        for entry_id, fields in entries:
            events.append(_entry_to_event(entry_id, fields))
    return events


async def read_live_transcript_events_async(
    uid: str,
    conversation_id: str,
    last_event_id: str,
    *,
    block_ms: int = STREAM_READ_BLOCK_MS,
    count: int = STREAM_READ_COUNT,
) -> List[Dict[str, Any]]:
    return await run_blocking(
        db_executor,
        read_live_transcript_events,
        uid,
        conversation_id,
        last_event_id,
        block_ms=block_ms,
        count=count,
    )
