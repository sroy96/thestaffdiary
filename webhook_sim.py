"""
Discrete-event model of one Kafka partition feeding a webhook dispatcher.
Compares a classic consumer group (one consumer owns the partition, commits in order)
with a share group (N consumers acquire records from the same partition).
Not a Kafka client: it models the delivery rules so the effect is measurable.
"""
import heapq, random, statistics, sys

random.seed(42)
TIMEOUT = 10.0          # HTTP timeout to merchant, seconds
HEALTHY = (0.05, 0.15)  # healthy merchant response time range, seconds
RATE = 10               # events per second on partition 7
DURATION = 60           # seconds of traffic
MERCHANTS = ["chaicart", "dosa_hub", "kirana_one", "pune_prints", "bookbazaar", "tiffinwala"]
DELIVERY_LIMIT = 5      # group.share.delivery.count.limit default

def make_events():
    ev, t, off = [], 0.0, 0
    while t < DURATION:
        m = random.choices(MERCHANTS, weights=[1, 3, 3, 3, 3, 3])[0]
        ev.append({"offset": off, "merchant": m, "produced": t})
        off += 1
        t += random.expovariate(RATE)
    return ev

def call_time(merchant):
    return TIMEOUT if merchant == "chaicart" else random.uniform(*HEALTHY)

def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs)-1, int(p/100*len(xs)))]

def classic(events):
    # One consumer, in-order, retries the failing record 3 times before giving up
    # (a common in-process retry policy), then moves on.
    clock, done = 0.0, {}
    for e in events:
        clock = max(clock, e["produced"])
        attempts = 3 if e["merchant"] == "chaicart" else 1
        for _ in range(attempts):
            clock += call_time(e["merchant"])
        done[e["offset"]] = clock
    return done

def share(events, consumers, breaker=False):
    # Each consumer acquires the lowest AVAILABLE record. ChaiCart calls time out,
    # the consumer RELEASEs, delivery count goes up, ARCHIVED at the limit.
    avail = []           # heap of (ready_time, offset)
    for e in events: heapq.heappush(avail, (e["produced"], e["offset"]))
    by_off = {e["offset"]: e for e in events}
    count = {e["offset"]: 0 for e in events}
    free = [0.0] * consumers
    done, archived, fails = {}, [], 0
    while avail:
        i = min(range(consumers), key=lambda k: free[k])
        ready, off = heapq.heappop(avail)
        start = max(free[i], ready)
        e = by_off[off]; count[off] += 1
        if breaker and e["merchant"] == "chaicart" and fails >= 3:
            free[i] = start + 0.001                     # breaker open: REJECT, no HTTP call
            archived.append(off); done[off] = free[i]; continue
        t = call_time(e["merchant"])
        free[i] = start + t
        if e["merchant"] == "chaicart":
            fails += 1
            if count[off] >= DELIVERY_LIMIT:
                archived.append(off); done[off] = free[i]
            else:
                heapq.heappush(avail, (free[i], off))   # RELEASE
        else:
            done[off] = free[i]                          # ACCEPT
    return done, archived

def report(name, events, done):
    lat = [done[e["offset"]] - e["produced"] for e in events if e["merchant"] != "chaicart"]
    last = max(done.values())
    print(f"{name}")
    print(f"  healthy merchants  p50={pct(lat,50):7.2f}s  p99={pct(lat,99):7.2f}s  max={max(lat):7.2f}s")
    print(f"  partition drained at t={last:7.1f}s (traffic stopped at t={DURATION}s)")

if __name__ == "__main__":
    events = make_events()
    n_chai = sum(e["merchant"] == "chaicart" for e in events)
    print(f"partition 7: {len(events)} events in {DURATION}s, {n_chai} for chaicart (endpoint hangs {TIMEOUT:.0f}s)\n")
    report("classic consumer group (1 consumer owns partition 7)", events, classic(events))
    print()
    for n in (6, 12):
        done, arch = share(events, n)
        report(f"share group ({n} consumers on partition 7)", events, done)
        print(f"  chaicart records archived after {DELIVERY_LIMIT} attempts: {len(arch)}")
        print()
    done, arch = share(events, 12, breaker=True)
    report("share group (12 consumers) + per-merchant breaker -> REJECT", events, done)
    print(f"  chaicart records rejected or archived: {len(arch)}")
