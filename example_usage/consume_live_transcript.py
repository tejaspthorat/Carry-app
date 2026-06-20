"""Consume Carry live transcript events from the POC global WebSocket API.

This intentionally uses the no-auth, no-conversation-id endpoint:
    /v4/live/transcripts

Usage:
    $env:CARRY_BACKEND_WS_URL="https://your-ngrok-url.ngrok-free.app"
    py example_usage\consume_live_transcript.py
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
from pathlib import Path

import websockets


BACKEND_URL = os.getenv('CARRY_BACKEND_WS_URL', 'https://aa22-42-104-224-81.ngrok-free.app')
LAST_ID_FILE = Path('.last_live_transcript_global_event_id')


def normalize_ws_url(url: str) -> str:
    url = url.rstrip('/')
    if url.startswith('https://'):
        return 'wss://' + url[len('https://') :]
    if url.startswith('http://'):
        return 'ws://' + url[len('http://') :]
    return url


def get_last_event_id() -> str:
    if LAST_ID_FILE.exists():
        value = LAST_ID_FILE.read_text(encoding='utf-8').strip()
        if value:
            return value
    return '0-0'


def save_last_event_id(event_id: str):
    LAST_ID_FILE.write_text(event_id, encoding='utf-8')


def ws_connect(url: str):
    headers = {'ngrok-skip-browser-warning': 'true'}
    params = inspect.signature(websockets.connect).parameters

    if 'additional_headers' in params:
        return websockets.connect(url, additional_headers=headers, ping_interval=20, ping_timeout=20)

    return websockets.connect(url, extra_headers=headers, ping_interval=20, ping_timeout=20)


def print_segments(segments):
    for segment in segments or []:
        speaker = segment.get('speaker') or f"SPEAKER_{segment.get('speaker_id', 0)}"
        text = segment.get('text', '')
        print(f'{speaker}: {text}')

        for translation in segment.get('translations') or []:
            lang = translation.get('lang')
            translated_text = translation.get('text')
            print(f'  [{lang}] {translated_text}')


async def consume():
    last_event_id = get_last_event_id()
    base_ws_url = normalize_ws_url(BACKEND_URL)
    url = f'{base_ws_url}/v4/live/transcripts?last_event_id={last_event_id}'

    print(f'Connecting: {url}')

    async with ws_connect(url) as ws:
        async for raw in ws:
            event = json.loads(raw)
            event_type = event.get('type')

            event_id = event.get('id')
            if event_id:
                save_last_event_id(event_id)

            if event_type == 'live_transcript_connected':
                print(f"Connected to {event.get('scope')} stream")

            elif event_type == 'transcript.updated':
                print('\n--- transcript.updated ---')
                print_segments(event.get('segments'))

            elif event_type == 'translation.ready':
                print('\n--- translation.ready ---')
                print_segments(event.get('segments'))

            elif event_type == 'conversation.started':
                print(f"\n--- conversation.started: {event.get('conversation_id')} ---")

            elif event_type == 'transcript.deleted':
                print('\n--- transcript.deleted ---')
                print(event.get('segment_ids'))

            elif event_type == 'live_transcript_heartbeat':
                print('.', end='', flush=True)

            else:
                print('\n--- raw event ---')
                print(json.dumps(event, indent=2))


async def main():
    while True:
        try:
            await consume()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f'\nDisconnected/error: {e}')
            print('Reconnecting in 3 seconds...')
            await asyncio.sleep(3)


if __name__ == '__main__':
    asyncio.run(main())
