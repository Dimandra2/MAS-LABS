import asyncio
import random
import statistics
from dataclasses import dataclass
from typing import Optional

MODEL_MINUTE = 0.002
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
    def __init__(self, aid, bus):
        self.id = aid
        self.bus = bus
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


class Auditor(Agent):
    def __init__(self, aid, bus, courier_ids):
        super().__init__(aid, bus)
        self.successes = {c: 0 for c in courier_ids}
        self.total = {c: 0 for c in courier_ids}
        self.courier_workload = {c: 0 for c in courier_ids}

    async def handle(self, msg):
        if msg["performative"] == "report_delivery":
            c = msg["content"]
            courier = c["courier"]
            is_late = c["is_late"]
            is_failed = c["is_failed"]

            self.courier_workload[courier] += 1
            self.total[courier] += 1
            if not is_late and not is_failed:
                self.successes[courier] += 1

            # Оценка Лапласа
            score = (self.successes[courier] + 1) / (self.total[courier] + 2)
            await self.send("dispatcher", "inform_reputation",
                            {"courier": courier, "score": score, "trials": self.total[courier]})


class Dispatcher(Agent):
    def __init__(self, aid, bus, mode, courier_ids, config):
        super().__init__(aid, bus)
        self.mode = mode
        self.courier_ids = courier_ids
        self.config = config
        self.orders = {}
        self.courier_load = {c: 0 for c in courier_ids}
        self.pending_proposals = {}
        self.retry_count = {}
        self.reputations = {c: 0.5 for c in courier_ids}
        self.trials = {c: 0 for c in courier_ids}

        self.high_to_good = 0
        self.high_to_bad = 0
        self.high_to_unproven = 0

    async def handle(self, msg):
        perf = msg["performative"]
        if perf == "request":
            await self.on_request(msg)
        elif perf == "propose":
            self.on_propose(msg)
        elif perf == "inform":
            await self.on_inform(msg)
        elif perf == "inform_reputation":
            self.reputations[msg["content"]["courier"]] = msg["content"]["score"]
            self.trials[msg["content"]["courier"]] = msg["content"]["trials"]

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
        await self.distribute_contract_net(order)

    def available_couriers(self):
        return [c for c in self.courier_ids if self.courier_load[c] < self.config["courier_capacity"]]

    def get_courier_weight(self, cid):
        # Если меньше 5 заказов, курьер считается "нейтральным" (0.5), чтобы получить шанс
        if self.trials[cid] < 5:
            return 0.5
        return self.reputations[cid]

    async def distribute_contract_net(self, order):
        cands = self.available_couriers()
        if not cands:
            await self.schedule_retry(order)
            return

        self.pending_proposals[order.id] = []
        for c in cands:
            await self.send(c, "cfp", {"order_id": order.id, "order": order.__dict__, "mode": "contract_net"})
        await asyncio.sleep(self.config["T_cfp"])

        proposals = self.pending_proposals.pop(order.id, [])
        if not proposals:
            await self.schedule_retry(order)
            return

        if self.mode == "reputation_priority" and order.priority == "high":
            # Сортируем: сначала по весу (убывание), потом по ETA (возрастание)
            best = max(proposals, key=lambda x: (self.get_courier_weight(x[0]), -x[1]))

            w = self.get_courier_weight(best[0])
            if w > 0.6:
                self.high_to_good += 1
            elif w == 0.5:
                self.high_to_unproven += 1
            else:
                self.high_to_bad += 1
        else:
            # Baseline: только ETA
            best = min(proposals, key=lambda x: x[1])
            if order.priority == "high":
                w = self.get_courier_weight(best[0])
                if w > 0.6:
                    self.high_to_good += 1
                elif w == 0.5:
                    self.high_to_unproven += 1
                else:
                    self.high_to_bad += 1

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
            self.pending_proposals[oid].append((msg["sender"], msg["content"]["value"], msg["content"]["load"]))

    async def on_inform(self, msg):
        c = msg["content"]
        if c.get("status") == "delivered":
            oid = c["order_id"]
            order = self.orders[oid]
            order.status = "delivered"
            order.delivered_at = now_model()
            if order.assigned_to:
                self.courier_load[order.assigned_to] -= 1

            is_late = c["is_late"]
            await self.send("auditor", "report_delivery",
                            {"courier": c["courier_id"], "is_late": is_late, "is_failed": False})

            await self.send(order.client_id, "inform", {"order_id": oid, "status": "delivered"})

    async def schedule_retry(self, order):
        self.retry_count[order.id] = self.retry_count.get(order.id, 0) + 1
        if self.retry_count[order.id] > self.config["N_retry"]:
            order.status = "failed"
            if order.assigned_to:
                await self.send("auditor", "report_delivery",
                                {"courier": order.assigned_to, "is_late": False, "is_failed": True})
            await self.send(order.client_id, "inform", {"order_id": order.id, "status": "failed"})
            return
        await asyncio.sleep(self.config["T_retry"])
        await self.distribute(order)


class Courier(Agent):
    def __init__(self, aid, bus, capacity, reliability, seed):
        super().__init__(aid, bus)
        self.capacity = capacity
        self.current_load = 0
        self.status = "free"
        self.reliability = reliability
        self.rng = random.Random(seed)

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

        eta = order["estimated_time"] * (1 + self.current_load * 0.2) + random.uniform(0, 5)
        await self.send(msg["sender"], "propose", {"order_id": order["id"], "value": eta, "load": self.current_load})

    async def on_accept(self, msg):
        order = msg["content"]["order"]
        self.current_load += 1
        if self.current_load >= self.capacity: self.status = "busy"
        asyncio.create_task(self.deliver(order, msg["sender"]))

    async def deliver(self, order, dispatcher_id):
        actual_time = order["estimated_time"]
        is_late = False

        if self.rng.random() > self.reliability:
            actual_time += 70
            is_late = True

        await asyncio.sleep(actual_time * MODEL_MINUTE)
        self.current_load -= 1
        if self.current_load == 0: self.status = "free"

        await self.send(dispatcher_id, "inform",
                        {"order_id": order["id"], "status": "delivered", "courier_id": self.id, "is_late": is_late})


class Client(Agent):
    def __init__(self, aid, bus, dispatcher_id, spec):
        super().__init__(aid, bus)
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
    random.seed(seed)
    bus = Bus()

    config = {"T_cfp": 1 * MODEL_MINUTE, "T_retry": 2 * MODEL_MINUTE, "N_retry": 20, "courier_capacity": 2}

    n_orders = 200
    reliabilities = [0.95, 0.85, 0.70, 0.40, 0.20, 0.10]
    n_couriers = len(reliabilities)

    couriers = [Courier(f"courier_{i}", bus, config["courier_capacity"], reliabilities[i], seed + i) for i in
                range(n_couriers)]
    dispatcher = Dispatcher("dispatcher", bus, mode, [c.id for c in couriers], config)
    auditor = Auditor("auditor", bus, [c.id for c in couriers])

    specs = []
    for i in range(n_orders):
        arrival = i * 0.5
        priority = "high"
        est_time = random.randint(5, 20)
        specs.append({
            "order_id": f"O-{i}", "pickup": f"A{i}", "dropoff": f"B{i}",
            "priority": priority, "estimated_time": est_time,
            "deadline": arrival + est_time + 60,
            "arrival": arrival, "client_id": f"client_{i}"
        })

    clients = [Client(s["client_id"], bus, "dispatcher", s) for s in specs]

    START_TIME = asyncio.get_event_loop().time()
    tasks = [asyncio.create_task(a.run()) for a in couriers + [dispatcher, auditor] + clients]

    try:
        await asyncio.wait_for(asyncio.gather(*[c.done.wait() for c in clients]), timeout=300)
    except asyncio.TimeoutError:
        pass

    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    orders = list(dispatcher.orders.values())
    high_priority_orders = [o for o in orders if o.priority == "high"]

    high_delivered = [o for o in high_priority_orders if o.status == "delivered" and o.delivered_at <= o.deadline]
    high_failed = [o for o in high_priority_orders if
                   o.status == "failed" or (o.status == "delivered" and o.delivered_at > o.deadline)]

    workload = list(auditor.courier_workload.values())
    fairness_std = statistics.pstdev(workload) if len(workload) > 1 else 0

    return {
        "mode": mode,
        "total_high": len(high_priority_orders),
        "high_on_time": len(high_delivered),
        "high_late_or_failed": len(high_failed),
        "workload_std": fairness_std,
        "workloads": auditor.courier_workload,
        "final_reps": dispatcher.reputations,
        "reliabilities": reliabilities,
        "trials": dispatcher.trials,
        "high_to_good": dispatcher.high_to_good,
        "high_to_bad": dispatcher.high_to_bad,
        "high_to_unproven": dispatcher.high_to_unproven
    }


async def main():
    print("--- СРАВНЕНИЕ: ДО И ПОСЛЕ ВВЕДЕНИЯ МЕХАНИЗМА (200 HIGH ЗАКАЗОВ) ---")
    r_base = await run_experiment("baseline", seed=42)
    r_rep = await run_experiment("reputation_priority", seed=42)

    print(f"{'Режим':<22} | {'Всего High':>12} | {'Вовремя':>14} | {'Срыв':>12} | {'Разброс загрузки':>16}")
    print("-" * 85)

    for r in [r_base, r_rep]:
        print(
            f"{r['mode']:<22} | {r['total_high']:>12} | {r['high_on_time']:>14} | {r['high_late_or_failed']:>12} | {r['workload_std']:>16.2f}")

    print("\n--- ДЕБАГ: РАСПРЕДЕЛЕНИЕ HIGH-ЗАКАЗОВ ---")
    print(f"{'Режим':<22} | {'Дали хорошим':>15} | {'Дали плохим':>15} | {'Дали новичкам':>15}")
    print("-" * 85)
    for r in [r_base, r_rep]:
        print(f"{r['mode']:<22} | {r['high_to_good']:>15} | {r['high_to_bad']:>15} | {r['high_to_unproven']:>15}")

    print("\n--- ДОКАЗАТЕЛЬСТВО ЭФФЕКТИВНОСТИ ---")
    print(f"{'Курьер':<12} | {'Истинная надежность':>20} | {'Оценка Аудитора':>15} | {'Заказов (триалов)':>17}")
    print("-" * 75)
    for i in range(len(r_rep['reliabilities'])):
        c_id = f"courier_{i}"
        true_rel = r_rep['reliabilities'][i]
        rep = r_rep['final_reps'].get(c_id, 0.5)
        trials = r_rep['trials'].get(c_id, 0)
        print(f"{c_id:<12} | {true_rel:>20.2f} | {rep:>15.2f} | {trials:>17}")


if __name__ == "__main__":
    asyncio.run(main())