from __future__ import annotations
import asyncio

import json
import logging
from datetime import datetime, UTC
from enum import Enum, IntEnum
from typing import TYPE_CHECKING

from aiohttp import ClientWebSocketResponse as WebSocketResponse

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

    CHAT_MESSAGE = "App\\Events\\ChatMessageEvent"
    STREAMER_IS_LIVE = "App\\Events\\StreamerIsLive"
    STOP_STREAM_BROADCAST = "App\\Events\\StopStreamBroadcast"
    FOLLOWERS_UPDATED = "App\\Events\\FollowersUpdated"


class PusherOPs(Enum):
    """Enum representing all the operations that can be sent to the Pusher WebSocket."""

    HEARTBEAT = "pusher:ping"
    SUBSCRIBE = "pusher:subscribe"
    UNSUBSCRIBE = "pusher:unsubscribe"


class PusherErrors(IntEnum):
    """Enum representing all the errors that can be received from the Pusher WebSocket."""

    RECONNECT = 4200


class PusherWebSocket:
    def __init__(self, ws: WebSocketResponse, *, http: HTTPClient):
        self.ws = ws
        self.http = http
        self.send_json = ws.send_json
        self.close = ws.close

        self._socket_id: str = ""
        self._heartbeat_timeout: int = 0
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_last_sent: datetime | None = None
        self._latency: float = 0.0

    @property
    def latency(self) -> float:
        return self._latency

    async def heartbeat_loop(self) -> None:
        while True:
            await self.send_json({"event": PusherOPs.HEARTBEAT.value, "data": {}})
            self._heartbeat_last_sent = datetime.now(UTC)

            await asyncio.sleep(self._heartbeat_timeout - 5)

    async def poll_event(self) -> None:
        raw_msg = await self.ws.receive()
        if raw_msg.data is None:
            return

        raw_data = raw_msg.json()
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
            case PusherEvents.CONNECTED:
                self._heartbeat_timeout = data["activity_timeout"]
                self._socket_id = data["socket_id"]
                self._heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                self.http.client.dispatch("ready")
            case PusherEvents.PONG:
                self._latency = (datetime.now(UTC) - self._heartbeat_last_sent).total_seconds()
                LOGGER.debug("Heartbeat received. Latency: %s", self._latency)
            case PusherEvents.ERROR:
                # TODO: Implement
                LOGGER.error("Pusher error: %s", data)
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

    async def start(self) -> None:
        while not self.ws.closed:
            await self.poll_event()

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
