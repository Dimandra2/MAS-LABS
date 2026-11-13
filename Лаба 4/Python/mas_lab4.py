import heapq
import random
import statistics
from dataclasses import dataclass, field
from typing import Optional, Callable


# ==========================================
# ЯДРО ДИСКРЕТНО-СОБЫТИЙНОЙ СИМУЛЯЦИИ (DES)
# ==========================================
@dataclass(order=True)
class Event:
    time: float
    counter: int
    action: Callable = field(compare=False)
    args: tuple = field(compare=False)


class Simulator:
    def __init__(self, seed):
        self.now = 0.0
        self.event_counter = 0
        self.events = []
        self.rng = random.Random(seed)

    def schedule(self, delay, action, *args):
        self.event_counter += 1
        heapq.heappush(self.events, Event(self.now + delay, self.event_counter, action, args))

    def run(self):
        while self.events:
            event = heapq.heappop(self.events)
            self.now = event.time
            event.action(*event.args)


# ==========================================
# ДОМЕННЫЕ МОДЕЛИ
# ==========================================
@dataclass
class Order:
    id: str
    estimated_time: int
    deadline: float
    status: str = "pending"
    assigned_at: float = 0.0
    assigned_to: Optional[str] = None
    delivered_at: Optional[float] = None


class Courier:
    def __init__(self, aid, sim, reliability, is_liar=False, liar_type=None, is_saboteur=False):
        self.id = aid
        self.sim = sim
        self.reliability = reliability
        self.is_liar = is_liar
        self.liar_type = liar_type
        self.is_saboteur = is_saboteur
        self.current_load = 0
        self.capacity = 3
        self.fails = 0  # Счетчик разоблачений (для блокировки)

    def receive_cfp(self, order):
        if self.current_load >= self.capacity or self.fails >= 3:
            return None
        return order.estimated_time * (1 + self.sim.rng.uniform(0.0, 0.1))

    def accept_order(self, order, dispatcher):
        self.current_load += 1
        order.assigned_to = self.id
        order.assigned_at = self.sim.now

        if self.is_saboteur:
            self.sim.schedule(1000, self.deliver, order, dispatcher, False)
            return

        actual_time = order.estimated_time
        is_late = False

        # ЖЕСТКИЙ ДЕТЕРМИНИЗМ: сбой зависит только от ID курьера и заказа
        local_rng = random.Random(f"fail_{self.id}_{order.id}")
        if local_rng.random() > self.reliability:
            actual_time += 40
            is_late = True

        # ЛОГИКА ЛЖЕЦОВ
        if self.is_liar:
            if self.liar_type == "fast":
                actual_time = 1.0
            elif self.liar_type == "realistic":
                actual_time = order.estimated_time
            elif self.liar_type == "huge":
                actual_time = order.estimated_time * 10
            is_late = False

        self.sim.schedule(actual_time, self.deliver, order, dispatcher, is_late)

    def deliver(self, order, dispatcher, is_late):
        self.current_load -= 1
        dispatcher.on_courier_report(order, self, is_late)


class Client:
    def __init__(self, aid, sim, dispatcher):
        self.id = aid
        self.sim = sim
        self.dispatcher = dispatcher

    def confirm_delivery(self, order, courier):
        # Клиент проверяет: реально ли курьер привез?
        if courier.is_liar or courier.is_saboteur:
            self.dispatcher.on_client_report(order, courier, received=False)
        else:
            self.dispatcher.on_client_report(order, courier, received=True)


class Dispatcher:
    def __init__(self, sim, mode, couriers, use_defense):
        self.sim = sim
        self.mode = mode
        self.couriers = couriers
        self.use_defense = use_defense

        self.successes = {c.id: 0 for c in couriers}
        self.total = {c.id: 0 for c in couriers}
        self.reputations = {c.id: 0.5 for c in couriers}

        self.orders = []
        self.liar_caught = 0
        self.saboteur_caught = 0

    def request(self, order, client):
        self.orders.append((order, client))
        self.try_assign(order, client)

    def try_assign(self, order, client):
        if order.status != "pending": return

        cands = [c for c in self.couriers if c.current_load < c.capacity and c.fails < 3]
        if not cands:
            self.sim.schedule(2.0, self.try_assign, order, client)
            return

        proposals = []
        for c in cands:
            eta = c.receive_cfp(order)
            if eta is not None:
                proposals.append((c, eta))

        if not proposals:
            self.sim.schedule(2.0, self.try_assign, order, client)
            return

        if self.mode == "reputation_priority":
            best = max(proposals, key=lambda x: (self.reputations[x[0].id], -x[1]))
        else:
            best = min(proposals, key=lambda x: x[1])

        chosen = best[0]
        order.status = "assigned"
        chosen.accept_order(order, self)

        if self.use_defense:
            expected_time = order.estimated_time * 2.0 + 50
            self.sim.schedule(expected_time, self.watchdog_check, order, client)

    def watchdog_check(self, order, client):
        if order.status == "assigned":
            order.status = "pending"
            if order.assigned_to:
                if self.saboteur_caught < 5:
                    print(f"[TRACE] Watchdog: Саботаж! {order.assigned_to} завис на заказе {order.id}.")
                self.saboteur_caught += 1

                self.total[order.assigned_to] += 10
                self.reputations[order.assigned_to] = (self.successes[order.assigned_to] + 1) / (
                            self.total[order.assigned_to] + 2)

                for c in self.couriers:
                    if c.id == order.assigned_to:
                        c.fails += 1
                        c.current_load = max(0, c.current_load - 1)
                        break
            self.try_assign(order, client)

    def on_courier_report(self, order, courier, is_late):
        if self.use_defense:
            order.status = "waiting_client"
            self.sim.schedule(0.1, self.ask_client, order, courier)
        else:
            self.finalize_delivery(order, courier, is_late)

    def ask_client(self, order, courier):
        client = next(c for o, c in self.orders if o.id == order.id)
        client.confirm_delivery(order, courier)

    def on_client_report(self, order, courier, received):
        if not received:
            if self.liar_caught < 5:
                print(
                    f"[TRACE] Client Check: Ложь! {courier.id} сказал, что доставил {order.id}, но клиент не получил.")
            self.liar_caught += 1

            courier.fails += 1
            self.total[courier.id] += 10
            self.reputations[courier.id] = (self.successes[courier.id] + 1) / (self.total[courier.id] + 2)

            order.status = "pending"
            order.assigned_to = None
            client = next(c for o, c in self.orders if o.id == order.id)
            self.sim.schedule(0.1, self.try_assign, order, client)
            return

        is_late = self.sim.now > order.deadline
        self.finalize_delivery(order, courier, is_late)

    def finalize_delivery(self, order, courier, is_late):
        order.status = "delivered"
        order.delivered_at = self.sim.now

        self.total[courier.id] += 1
        if not is_late:
            self.successes[courier.id] += 1

        self.reputations[courier.id] = (self.successes[courier.id] + 1) / (self.total[courier.id] + 2)


# ==========================================
# ЗАПУСК ЭКСПЕРИМЕНТА
# ==========================================
def run_experiment(mode, use_defense, seed=42):
    sim = Simulator(seed)

    # ЧЕСТНЫЕ КУРЬЕРЫ (Высокая надежность 0.90 - 0.99)
    couriers = [
        Courier("courier_0", sim, 0.99),
        Courier("courier_1", sim, 0.98),
        Courier("courier_2", sim, 0.97),
        Courier("courier_3", sim, 0.96),
        Courier("courier_4", sim, 0.95),
        Courier("courier_5", sim, 0.94),
        Courier("courier_6", sim, 0.93),
        Courier("courier_7", sim, 0.92),
        Courier("courier_8", sim, 0.91),
        Courier("courier_9", sim, 0.90),
        # ДИВЕРСАНТЫ
        Courier("courier_10_liar_fast", sim, 0.80, is_liar=True, liar_type="fast"),
        Courier("courier_11_liar_real", sim, 0.80, is_liar=True, liar_type="realistic"),
        Courier("courier_12_liar_huge", sim, 0.80, is_liar=True, liar_type="huge"),
        Courier("courier_13_saboteur", sim, 0.50, is_saboteur=True)
    ]

    dispatcher = Dispatcher(sim, mode, couriers, use_defense)

    n_orders = 500
    client = Client("client_0", sim, dispatcher)

    def request_order(i, est_time):
        order = Order(
            id=f"O-{i}",
            estimated_time=est_time,
            deadline=sim.now + est_time + 40
        )
        dispatcher.request(order, client)

    for i in range(n_orders):
        est_time = sim.rng.randint(10, 20)
        sim.schedule(i * 1.0, request_order, i, est_time)

    sim.run()

    true_success = 0
    total_failed = 0

    for o, _ in dispatcher.orders:
        if o.status == "delivered" and o.delivered_at <= o.deadline:
            courier = next(c for c in couriers if c.id == o.assigned_to)
            if not courier.is_liar:
                true_success += 1
            else:
                total_failed += 1
        else:
            total_failed += 1

    reliabilities = {c.id: c.reliability for c in couriers}

    return {
        "mode": mode,
        "defense": use_defense,
        "true_success": true_success,
        "failed": total_failed,
        "reputations": dispatcher.reputations,
        "reliabilities": reliabilities
    }


def main():
    SEED = 42
    print(f"--- ДИВЕРСИОННЫЙ АНАЛИЗ МАС (SEED = {SEED}, 500 заказов, 14 курьеров) ---")
    print("Сценарии: 1 Наглый лжец, 1 Умный лжец, 1 Медленный лжец, 1 Саботажник\n")

    print(">>> ЗАПУСК УЯЗВИМОЙ СИСТЕМЫ (БЕЗ ЗАЩИТЫ) <<<")
    r_vuln = run_experiment("baseline", use_defense=False, seed=SEED)
    print("\n")

    print(">>> ЗАПУСК ЗАЩИЩЕННОЙ СИСТЕМЫ (С ПРОВЕРКОЙ КЛИЕНТА) <<<")
    r_prot = run_experiment("baseline", use_defense=True, seed=SEED)

    print("\n" + "=" * 85)
    print("ИТОГОВАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ:")
    print(f"{'Конфигурация':<30} | {'Успешно доставлено':>20} | {'Сорвано/Обмануто':>20}")
    print("-" * 85)
    print(f"{'Уязвимая (Без защиты)':<30} | {r_vuln['true_success']:>20} | {r_vuln['failed']:>20}")
    print(f"{'Защищенная (С защитой)':<30} | {r_prot['true_success']:>20} | {r_prot['failed']:>20}")

    print("\n--- СРАВНЕНИЕ ИСТИННОЙ НАДЕЖНОСТИ И РЕПУТАЦИИ (В защищенной системе) ---")
    print(f"{'Агент':<25} | {'Истинная надежность':>20} | {'Оценка Аудитора':>20}")
    print("-" * 75)
    for cid, rep in r_prot['reputations'].items():
        true_rel = r_prot['reliabilities'].get(cid, 0.0)
        print(f"{cid:<25} | {true_rel:>20.2f} | {rep:>20.2f}")


if __name__ == "__main__":
    main()