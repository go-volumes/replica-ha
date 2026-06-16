# replica-ha

High-availability **control plane** for [go-volumes](https://github.com/go-volumes)
replicated block volumes. It drives a [`replica.Engine`](https://github.com/go-volumes/replica)
(the data plane that mirrors a volume across N synchronous replicas) under
**leader election with fence-before-promote (STONITH)**, so that **exactly one
node is the active writer for a volume at any instant** — the safe
single-active-writer guarantee that makes failover non-destructive.

- Pure Go, `CGO_ENABLED=0`, no external runtime.
- Dependency-free core: it imports only `go-volumes/replica` and
  `go-volumes/interface`. The coordination store is behind an interface, so the
  safety-critical state machine carries no etcd weight and is exhaustively unit
  tested (100% coverage) with an in-memory fake.

## The safety property

A replicated volume is only safe if **at most one writer is live at a time**. If
two nodes both believe they own the volume — the classic split-brain outcome of
a network partition where a stale leader keeps writing while a new leader is
elected on the other side — they mirror divergent writes into the same replicas
and corrupt the volume irrecoverably.

This package prevents that with **fence-before-promote**: a node that wins
leadership MUST *fence the previous writer* (prove it can no longer write)
**before** it activates its own engine as the writer. If the fence fails or
times out, the node refuses to activate and stays passive. The control plane
never writes while a possibly-live old writer might also be writing.

## Architecture

```
   coordinator (lease-based leader election + membership; etcd in production)
        │  Campaign / Resign / Observe / Members
        ▼
   ┌──────────────── Controller (per node) ────────────────┐
   │  observe leadership →                                  │
   │    won  → FENCE prior writer ──fail──▶ stay PASSIVE    │
   │                          └──ok──▶ open write gate      │
   │    lost → close write gate (demote immediately)        │
   └───────────────────────────────────────────────────────┘
        │ Device()
        ▼
   ActiveDevice  (volume.Device; WriteAt/Sync → ErrNotLeader when passive)
        │
        ▼
   replica.Engine  →  replica A, replica B, … (synchronous mirror)
```

### `Coordinator` — the coordination seam

```go
type Coordinator interface {
    Campaign(ctx context.Context) error
    Resign(ctx context.Context) error
    Observe(ctx context.Context) (<-chan Leadership, error)
    Members(ctx context.Context) ([]string, error)
    NodeID() string
}

type Leadership struct {
    Leader string // node ID of the lease holder ("" = none)
    IsSelf bool   // is the leader us? false-after-true = lease LOST
    Term   int64  // monotonic election term (fencing token)
}
```

Leadership is backed by a **time-bounded lease**. If a node stops renewing it
(process stall, partition, being fenced by a peer), the lease **expires** at the
store and a `Leadership{IsSelf:false}` MUST surface on `Observe` so the
`Controller` demotes and stops writing before the grace window elapses. An
etcd-backed `Coordinator` (mirroring weft-ha-postgresql's `EtcdDCS`, with an
embedded-etcd integration test) is the **deferred next step**; it lives outside
this core so the state machine stays dep-free.

### `Controller` — the reconcile state machine

```go
ctrl, dev, err := replicaha.New(engine, coord, fencer, logger)
go ctrl.Run(ctx)          // campaign, fence-before-promote, gate writes
// ... the data path (a filesystem-format driver, an NBD server) writes through dev
defer ctrl.Stop(ctx)      // resign + deactivate
```

Roles: `RoleFollower` (gate closed) → `RoleFencePending` (won the lease, **not
yet** writing — fencing the prior writer) → `RoleLeader` (fence confirmed, gate
open). A failed fence stays `RoleFencePending` and never opens the gate.

### Active/passive write gating

`replica.Engine` has no leadership notion, so the gate lives in front of it.
`ActiveDevice` wraps the engine and satisfies `volume.Device`:

- **passive** (default, and after losing leadership): `WriteAt` / `Sync` return
  `ErrNotLeader` without touching any replica; `ReadAt` / `Size` still pass
  through (a follower may serve reads).
- **active** (only after a confirmed fence): everything passes straight to the
  engine.

The data path needs no awareness of leadership beyond handling `ErrNotLeader`.

## Status

Core control plane complete: `Coordinator` seam, `Controller` state machine,
`ActiveDevice` gate, 100% test coverage on all 6 supported 64-bit
architectures. The etcd-backed `Coordinator` is the deferred follow-up.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-volumes/replica-ha authors.
