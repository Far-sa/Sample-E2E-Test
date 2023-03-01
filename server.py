import websockets
import asyncio
import aioredis
import json
import uuid

#! task ==> its a section for send/receive data


class E2EChatServer():

    def __init__(self) -> None:
        self.redis = aioredis.from_url("redis://localhost:6379")

    # client-side
    async def handler(self, websocket, path):
        async for message in websocket:
            try:
                data = json.loads(message)
            except:
                pass
            else:
                target_uid = data.get("target_uid", None)
                if target_uid:
                    await self.redis.publish(f"user:{target_uid}", message)

    async def redis_channel(self, websocket, data_channel):
        while True:
            message = await data_channel.get_message(ignore_subscribe_messages=True)
            if message:
                print(f"(Reader) Message Received: {message}")
                await websocket.send(message)
            await asyncio.sleep(0.01)

    async def __call__(self, websocket, path):
        pubsub = self.redis.pubsub()
        user_uid = str(uuid.uuid4())

        await websocket.send(json.dumps({"uid": user_uid}))
        await pubsub.subscribe(f"user:{user_uid}")

        #! client-side
        handler_task = asyncio.ensure_future(
            self.handler(websocket, path))
        #! server-side
        redis_channel_task = asyncio.ensure_future(
            self.redis_channel(pubsub, websocket))
        done, pending = await asyncio.wait(
            [handler_task, redis_channel_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()


#! Context Manager


async def main():
    ws_handler = E2EChatServer()
    async with websockets.serve(ws_handler, "localhost", 3000):
        print("server is running on localhost:3000")
        # run forever
        await asyncio.Future()

asyncio.run(main())
