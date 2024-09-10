from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from enum import Enum, IntEnum
from typing import TYPE_CHECKING

import aiohttp
import yarl
from aiohttp import ClientWebSocketResponse

from .livestream import PartialLivestream, PartialLivestreamStop
from .message import Message

if TYPE_CHECKING:
    from .http import HTTPClient

__all__ = ()

LOGGER = logging.getLogger(__name__)


class PusherEvents(Enum):
    """Enum representing all the events that can be received from the Pusher WebSocket."""

    CONNECTED = "pusher:connection_established"
    ERROR = "pusher:error"
    PONG = "pusher:pong"
    SUBSCRIPTION_SUCCEEDED = "pusher_internal:subscription_succeeded"
    SUBSCRIPTION_ERROR = "pusher_internal:subscription_error"

    CHAT_MESSAGE = "App\\Events\\ChatMessageEvent"
    STREAMER_IS_LIVE = "App\\Events\\StreamerIsLive"
    STOP_STREAM_BROADCAST = "App\\Events\\StopStreamBroadcast"
    FOLLOWERS_UPDATED = "App\\Events\\FollowersUpdated"


class PusherOPs(Enum):
    """Enum representing all the operations that can be sent to the Pusher WebSocket."""

    HEARTBEAT = "pusher:ping"
    SUBSCRIBE = "pusher:subscribe"
    UNSUBSCRIBE = "pusher:unsubscribe"


class PusherErrorsCodes(IntEnum):
    """Enum representing all the errors codes that can be received from the Pusher WebSocket."""

    UNKNOWN = -1
    RECONNECT = 4200


class PusherReconnect(Exception):
    """Exception raised when the Pusher WebSocket requests a reconnect."""


class PusherWebSocket:
    """Class representing the Pusher WebSocket."""

    BASE_URL = yarl.URL("wss://ws-us2.pusher.com/app/eb1d5f283081a78b932c")
    WS_URL = BASE_URL.with_query(protocol=7, client="js", version="8.4.0-rc2", flash="false")

    def __init__(self, http: HTTPClient) -> None:
        self.http: HTTPClient = http

        self.socket: ClientWebSocketResponse | None = None
        self._socket_id: str = ""
        self._heartbeat_timeout: int = 0
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_last_sent: datetime | None = None
        self._latency: float = 0.0
        self._worker_task: asyncio.Task | None = None

    async def close(self) -> None:
        self._cancel_tasks()
        if self.socket is not None:
            await self.socket.close(code=1000)

    def _cancel_tasks(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

    async def start(self) -> None:
        self.socket = await self.http._session.ws_connect(
            self.WS_URL, timeout=30, autoclose=False, max_msg_size=0
        )

        # Poll event for OP CONNECTED
        await self.poll_event()

        self._worker_task = asyncio.create_task(self.worker())

    async def worker(self) -> None:
        while not self.socket.closed:
            try:
                await self.poll_event()
            except (PusherReconnect, asyncio.TimeoutError, aiohttp.ClientError, OSError):
                await self.reconnect()

    async def reconnect(self) -> None:
        self._cancel_tasks()
        await self.start()
        for user in self.http.client._watched_users:
            await self.watch_channel(user)

    @property
    def latency(self) -> float:
        return self._latency

    async def heartbeat_loop(self) -> None:
        while not self.socket.closed:
            await self.send_json({"event": PusherOPs.HEARTBEAT.value, "data": {}})
            self._heartbeat_last_sent = datetime.now(UTC)
            await asyncio.sleep(self._heartbeat_timeout - 5)

    async def error_handler(self, error: PusherErrorsCodes, data: dict) -> None:
        match error:
            case PusherErrorsCodes.RECONNECT:
                LOGGER.warning("Pusher requested a reconnect. Reconnecting...")
                raise PusherReconnect
            case _:
                LOGGER.warning("Unknown Pusher error received: %s - %s", error, data)

    async def poll_event(self) -> None:
        raw_msg = await self.socket.receive(timeout=60)
        if raw_msg.type is aiohttp.WSMsgType.TEXT:
            await self.received_message(raw_msg)
        elif raw_msg.type is aiohttp.WSMsgType.BINARY:
            LOGGER.warning("Received unexpected binary data from Pusher: %s", raw_msg)
        elif raw_msg.type in (
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSE,
        ):
            raise PusherReconnect

    async def received_message(self, message: aiohttp.WSMessage) -> None:
        raw_data = message.json()
        data = (
            json.loads(raw_data["data"])
            if not isinstance(raw_data["data"], dict)
            else raw_data["data"]
        )

        self.http.client.dispatch("payload_receive", raw_data["event"], data)
        self.http.client.dispatch("raw_payload_receive", raw_data)

        LOGGER.debug("Received payload: %s", raw_data)

        try:
            event = PusherEvents(raw_data["event"])
        except ValueError:
            LOGGER.warning("Unknown event received: %s", raw_data["event"])
            return

        match event:
            # Pusher events
            case PusherEvents.CONNECTED:
                self._heartbeat_timeout = data["activity_timeout"]
                self._socket_id = data["socket_id"]
                self._heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                self.http.client.dispatch("ready")
            case PusherEvents.PONG:
                self._latency = (datetime.now(UTC) - self._heartbeat_last_sent).total_seconds()
                LOGGER.debug("Heartbeat received. Latency: %s", self._latency)
            case PusherEvents.ERROR:
                await self.error_handler(PusherErrorsCodes(data["code"] or -1), data)
                LOGGER.debug("Pusher error: %s", data)

            # Actual Kick events
            case PusherEvents.CHAT_MESSAGE:
                msg = Message(data=data, http=self.http)
                self.http.client.dispatch("message", msg)
            case PusherEvents.STREAMER_IS_LIVE:
                livestream = PartialLivestream(data=data["livestream"], http=self.http)
                self.http.client.dispatch("livestream_start", livestream)
            case PusherEvents.STOP_STREAM_BROADCAST:
                livestream = PartialLivestreamStop(data=data["livestream"], http=self.http)
                self.http.client.dispatch("livestream_stop", livestream)
            # case PusherEvents.FOLLOWERS_UPDATED:
            #     user = self.http.client._watched_users[data["channel_id"]]
            #     if data["followed"] is True:
            #         event = "follow"
            #         user._data["followers_count"] += 1
            #     else:
            #         event = "unfollow"
            #         user._data["followers_count"] -= 1

            #     self.http.client.dispatch(event, user)

    async def subscribe_to_chatroom(self, chatroom_id: int) -> None:
        await self.send_json(
            {
                "event": PusherOPs.SUBSCRIBE.value,
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }
        )

    async def unsubscribe_to_chatroom(self, chatroom_id: int) -> None:
        await self.send_json(
            {
                "event": PusherOPs.UNSUBSCRIBE.value,
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }
        )

    async def watch_channel(self, channel_id: int) -> None:
        await self.send_json(
            {
                "event": PusherOPs.SUBSCRIBE.value,
                "data": {"auth": "", "channel": f"channel.{channel_id}"},
            }
        )

    async def unwatch_channel(self, channel_id: int) -> None:
        await self.send_json(
            {
                "event": PusherOPs.UNSUBSCRIBE.value,
                "data": {"auth": "", "channel": f"channel.{channel_id}"},
            }
        )

    async def send_json(self, data: dict) -> None:
        try:
            await self.socket.send_json(data)
        except ConnectionResetError:
            await self.reconnect()
            raise PusherReconnect
