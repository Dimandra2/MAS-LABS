import asyncio
import random
import statistics
from dataclasses import dataclass
from typing import Optional

MODEL_MINUTE = 0.01
START_TIME = 0.0


def now_model() -> float:
    return (asyncio.get_event_loop().time() - START_TIME) / MODEL_MINUTE


class Bus:
    def __init__(self):
        self.queues = {}
        self.log = []
        self.lock = asyncio.Lock()

    def register(self, agent_id):
        self.queues[agent_id] = asyncio.Queue()

    async def send(self, msg):
        async with self.lock:
            self.log.append(msg)
        await self.queues[msg["receiver"]].put(msg)

    async def recv(self, agent_id):
        return await self.queues[agent_id].get()


class Agent:
    def __init__(self, aid, bus, seed):
        self.id = aid
        self.bus = bus
        self.rng = random.Random(seed)  # Личный генератор случайных чисел
        bus.register(aid)

    async def run(self):
        while True:
            msg = await self.bus.recv(self.id)
            await self.handle(msg)

    async def handle(self, msg):
        raise NotImplementedError

    async def send(self, receiver, performative, content):
        await self.bus.send({
            "sender": self.id, "receiver": receiver, "performative": performative,
            "content": content, "ts": now_model(),
        })


@dataclass
class Order:
    id: str
    pickup: str
    dropoff: str
    priority: str
    estimated_time: int
    deadline: float
    client_id: str
    status: str = "pending"
    created_at: float = 0.0
    delivered_at: Optional[float] = None
    assigned_to: Optional[str] = None


class Dispatcher(Agent):
    def __init__(self, aid, bus, mode, courier_ids, config, seed):
        super().__init__(aid, bus, seed)
        self.mode = mode
        self.courier_ids = courier_ids
        self.config = config
        self.orders = {}
        self.courier_load = {c: 0 for c in courier_ids}
        self.pending_proposals = {}
        self.retry_count = {}

    async def handle(self, msg):
        perf = msg["performative"]
        if perf == "request":
            await self.on_request(msg)
        elif perf == "propose":
            self.on_propose(msg)
        elif perf == "inform":
            await self.on_inform(msg)

    async def on_request(self, msg):
        c = msg["content"]
        order = Order(
            id=c["order_id"], pickup=c["pickup"], dropoff=c["dropoff"],
            priority=c["priority"], estimated_time=c["estimated_time"],
            deadline=c["deadline"], client_id=c["client_id"],
            created_at=now_model(),
        )
        self.orders[order.id] = order
        asyncio.create_task(self.distribute(order))

    async def distribute(self, order):
        if order.status != "pending": return
        if self.mode == "centralized":
            await self.distribute_centralized(order)
        elif self.mode in ("contract_net", "auction"):
            await self.distribute_bidding(order)

    def available_couriers(self):
        return [c for c in self.courier_ids if self.courier_load[c] < self.config["courier_capacity"]]

    async def distribute_centralized(self, order):
        cands = self.available_couriers()
        if not cands:
            await self.schedule_retry(order)
            return
        min_load = min(self.courier_load[c] for c in cands)
        best = [c for c in cands if self.courier_load[c] == min_load]
        chosen = self.rng.choice(best)  # Случайный выбор среди равных
        self.courier_load[chosen] += 1
        order.status = "assigned"
        order.assigned_to = chosen
        await self.send(chosen, "accept-proposal", {"order_id": order.id, "order": order.__dict__})

    async def distribute_bidding(self, order):
        cands = self.available_couriers()
        if not cands:
            await self.schedule_retry(order)
            return
        self.pending_proposals[order.id] = []
        for c in cands:
            await self.send(c, "cfp", {"order_id": order.id, "order": order.__dict__, "mode": self.mode})
        await asyncio.sleep(self.config["T_cfp"])

        proposals = self.pending_proposals.pop(order.id, [])
        if not proposals:
            await self.schedule_retry(order)
            return

        # Выбираем минимальную ставку (ETA для Contract Net, Цена для Auction)
        best = min(proposals, key=lambda x: x[1])
        chosen = best[0]
        self.courier_load[chosen] += 1
        order.status = "assigned"
        order.assigned_to = chosen

        await self.send(chosen, "accept-proposal", {"order_id": order.id, "order": order.__dict__})
        for c in cands:
            if c != chosen:
                await self.send(c, "reject-proposal", {"order_id": order.id})

    def on_propose(self, msg):
        oid = msg["content"]["order_id"]
        if oid in self.pending_proposals:
            self.pending_proposals[oid].append((msg["sender"], msg["content"]["value"]))

    async def on_inform(self, msg):
        c = msg["content"]
        if c.get("status") == "delivered":
            oid = c["order_id"]
            order = self.orders[oid]
            order.status = "delivered"
            order.delivered_at = now_model()
            if order.assigned_to:
                self.courier_load[order.assigned_to] -= 1
            await self.send(order.client_id, "inform", {"order_id": oid, "status": "delivered"})

    async def schedule_retry(self, order):
        self.retry_count[order.id] = self.retry_count.get(order.id, 0) + 1
        if self.retry_count[order.id] > self.config["N_retry"]:
            order.status = "failed"
            await self.send(order.client_id, "inform", {"order_id": order.id, "status": "failed"})
            return
        await asyncio.sleep(self.config["T_retry"])
        await self.distribute(order)


class Courier(Agent):
    def __init__(self, aid, bus, capacity, seed):
        super().__init__(aid, bus, seed)
        self.capacity = capacity
        self.current_load = 0
        self.status = "free"

    async def handle(self, msg):
        perf = msg["performative"]
        if perf == "cfp":
            await self.on_cfp(msg)
        elif perf == "accept-proposal":
            await self.on_accept(msg)

    async def on_cfp(self, msg):
        if self.current_load >= self.capacity:
            await self.send(msg["sender"], "refuse", {"order_id": msg["content"]["order_id"]})
            return
        order = msg["content"]["order"]
        mode = msg["content"]["mode"]

        base_time = order["estimated_time"] * (1 + self.current_load * 0.2)

        if mode == "contract_net":
            # Честная оценка времени + небольшой шум
            eta = base_time + self.rng.uniform(0, 2)
            await self.send(msg["sender"], "propose", {"order_id": order["id"], "value": eta})
        elif mode == "auction":
            # Жадность курьера: он хочет получить больше, поэтому завышает цену (случайный коэффициент)
            # Если заказов много, он может демпинговать
            greed = self.rng.uniform(0.8, 1.5)
            price = base_time * greed
            await self.send(msg["sender"], "propose", {"order_id": order["id"], "value": price})

    async def on_accept(self, msg):
        order = msg["content"]["order"]
        self.current_load += 1
        if self.current_load >= self.capacity: self.status = "busy"
        asyncio.create_task(self.deliver(order, msg["sender"]))

    async def deliver(self, order, dispatcher_id):
        await asyncio.sleep(order["estimated_time"] * MODEL_MINUTE)
        self.current_load -= 1
        if self.current_load == 0: self.status = "free"
        await self.send(dispatcher_id, "inform", {"order_id": order["id"], "status": "delivered"})


class Client(Agent):
    def __init__(self, aid, bus, dispatcher_id, spec, seed):
        super().__init__(aid, bus, seed)
        self.dispatcher_id = dispatcher_id
        self.spec = spec
        self.done = asyncio.Event()

    async def run(self):
        await asyncio.sleep(self.spec["arrival"] * MODEL_MINUTE)
        await self.send(self.dispatcher_id, "request", self.spec)
        while True:
            msg = await self.bus.recv(self.id)
            if msg["performative"] == "inform":
                if msg["content"].get("status") in ("delivered", "failed"):
                    self.done.set()
                    break


async def run_experiment(mode, seed=42):
    global START_TIME
    bus = Bus()

    config = {"T_cfp": 5 * MODEL_MINUTE, "T_retry": 10 * MODEL_MINUTE, "N_retry": 2, "courier_capacity": 1}
    n_orders = 40
    n_couriers = 3

    couriers = [Courier(f"courier_{i}", bus, config["courier_capacity"], seed + i) for i in range(n_couriers)]
    dispatcher = Dispatcher("dispatcher", bus, mode, [c.id for c in couriers], config, seed)

    specs = []
    rng = random.Random(seed)
    for i in range(n_orders):
        arrival = i * 1
        specs.append({
            "order_id": f"O-{i}", "pickup": f"A{i}", "dropoff": f"B{i}",
            "priority": rng.choice(["low", "normal", "high"]),
            "estimated_time": rng.randint(5, 20),
            "deadline": arrival + rng.randint(20, 50),
            "arrival": arrival, "client_id": f"client_{i}"
        })

    clients = [Client(s["client_id"], bus, "dispatcher", s, seed) for s in specs]

    START_TIME = asyncio.get_event_loop().time()
    tasks = [asyncio.create_task(a.run()) for a in couriers + [dispatcher] + clients]

    try:
        await asyncio.wait_for(asyncio.gather(*[c.done.wait() for c in clients]), timeout=120)
    except asyncio.TimeoutError:
        pass

    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    orders = list(dispatcher.orders.values())
    total = len(orders)
    delivered = [o for o in orders if o.status == "delivered"]
    failed = [o for o in orders if o.status == "failed"]
    on_time = [o for o in delivered if o.delivered_at <= o.deadline]
    late = [o for o in delivered if o.delivered_at > o.deadline]
    completion_times = [o.delivered_at - o.created_at for o in delivered]

    return {
        "mode": mode, "total": total, "on_time": len(on_time), "late": len(late),
        "failed": len(failed),
        "avg_completion": statistics.mean(completion_times) if completion_times else 0,
        "total_messages": len(bus.log),
        "messages_per_order": len(bus.log) / total if total else 0
    }


async def main():
    modes = ["centralized", "contract_net", "auction"]
    results = []
    for mode in modes:
        r = await run_experiment(mode, seed=42)
        results.append(r)

    print(
        f"{'Mode':<14} | {'Orders':>8} | {'OnTime':>8} | {'Late':>6} | {'Failed':>8} | {'AvgTime':>8} | {'Messages':>10} | {'Msg/Order':>10}")
    print("-" * 95)
    for r in results:
        print(
            f"{r['mode']:<14} | {r['total']:>8} | {r['on_time']:>8} | {r['late']:>6} | {r['failed']:>8} | {r['avg_completion']:>8.1f} | {r['total_messages']:>10} | {r['messages_per_order']:>10.1f}")


if __name__ == "__main__":
    asyncio.run(main())