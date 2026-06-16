# replica-ha

[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![CGO_ENABLED=0](https://img.shields.io/badge/CGO__ENABLED-0-success)](https://pkg.go.dev/cmd/cgo)

The **high-availability control plane** for [go-volumes](https://github.com/go-volumes)
replicated block volumes. It drives a
[`replica.Engine`](https://github.com/go-volumes/replica) (the data plane that
mirrors a volume across N synchronous replicas) under leader election so that
**exactly one node is the active writer** for a volume at any instant — the safe
single-active-writer guarantee that makes failover non-destructive.

It is deliberately **vendor-neutral**: leader election and fencing are
*interfaces you implement*, not a baked-in dependency. Bring etcd, Consul,
ZooKeeper, or a SQL advisory lock; bring micro-VM STONITH, IPMI, or a cloud
detach call. The safety-critical reconcile core depends only on the Go standard
library and `go-volumes/{replica,interface}` — **no consensus store is linked
in** — and is exhaustively unit-tested (100% coverage, 6 arches) against an
in-memory fake.

## The safety property it guarantees

A replicated volume is only safe if **at most one writer is live at a time**. If
two nodes both believe they own the volume (split-brain — the classic outcome of
a partition where a stale leader keeps writing while a new leader is elected on
the other side), they mirror divergent writes into the same replicas and corrupt
the volume irrecoverably.

`replica-ha` prevents this with **fence-before-promote (STONITH)**: a node that
wins leadership MUST *fence* the previous writer — prove it can no longer write —
**before** it activates its own engine as the writer. If the fence fails or
times out, the node **refuses to activate** and stays passive. The control plane
never writes while a possibly-live old writer might also be writing.

```
   Coordinator (lease election)                          Fencer
        │ Observe: IsSelf=true                              ▲
        ▼                                                   │ Fence(prevWriter)
 follower ─▶ fence-pending ──(fence OK)──▶ leader           │  MUST isolate the old
        │      (fence FAILS → stay fence-pending,           │  writer; nil ONLY if
        │       NEVER write — no split-brain)               │  that is truly done
        ▼ Observe: IsSelf=false (lease lost)                │
   demote ◀──────────────────────────────────────── ActiveDevice gate:
                                          WriteAt/Sync → ErrNotLeader when passive
        │
        ▼
   replica.Engine → replica A, replica B, …  (synchronous mirror)
```

## Pieces

- **`Coordinator`** — the coordination seam: lease-based leader election +
  membership. An interface, so the reconcile core is dependency-free and fully
  testable with an in-memory fake.
- **`Controller`** — the reconcile state machine: `RoleFollower` → `RoleFencePending`
  (won the lease, *not yet* writing) → `RoleLeader` (fence confirmed, gate open);
  demotes immediately on lease loss.
- **`ActiveDevice`** — wraps the `replica.Engine` so `WriteAt`/`Sync` return
  `ErrNotLeader` while passive (`ReadAt`/`Size` always pass through — a follower
  may still serve reads). This is how "active writer" is enforced on the data
  path; the engine itself has no leadership awareness.
- **`replica.Fencer`** — the fencing seam (defined in `go-volumes/replica`).

## Usage

```go
import (
    "github.com/go-volumes/replica"
    replicaha "github.com/go-volumes/replica-ha"
)

eng, _ := replica.New(replicas, replica.Config{MinInSync: 2, Local: "node-a"})

ctrl, dev, err := replicaha.New(eng, myCoordinator, myFencer, logger)
if err != nil {
    return err
}
// dev is the gated volume.Device the data path writes through — hand it to an
// NBD server, a filesystem-format driver, or pool.OpenWith. It rejects writes
// with ErrNotLeader until this node is the confirmed leader.
go ctrl.Run(ctx)                    // campaign, fence-before-promote, demote
defer ctrl.Stop(context.Background()) // resign + deactivate
```

## Implementing a `Coordinator`

A `Coordinator` exposes a **time-bounded lease** for one volume. The lease is the
safety hinge: while you hold it you may (after fencing) write; the instant you
stop renewing it — stall, partition, being fenced — it expires and you are no
longer leader. Implement it over any store with sessions/leases and a
compare-and-swap election: etcd `concurrency.Election`, Consul sessions,
ZooKeeper ephemeral znodes, a Postgres advisory lock with a TTL, …

| Method | Contract |
| --- | --- |
| `Campaign(ctx)` | Block until this node acquires and holds the lease, or `ctx` is done. Returning nil means **leadership is held now**. Re-campaigning while leader may return immediately. |
| `Resign(ctx)` | Release the lease so a peer can win. A no-op (nil) when not leader. |
| `Observe(ctx)` | Stream a `Leadership` on **every** change. **CRITICAL:** a lost lease (expiry/partition) MUST surface here as `IsSelf == false` so the Controller demotes *before the lease grace window elapses*. An implementation that cannot observe its own lease loss is **unsafe and unusable here**. Deliver the current state promptly so a late subscriber is not blind; close the channel when `ctx` is done or the session dies. |
| `Members(ctx)` | The node IDs holding a live membership lease — fenced/partitioned nodes drop out automatically as their lease expires. |
| `NodeID()` | This node's stable identity (matches `Leadership.Leader` when leader; the id a peer passes to `Fence`). |

`Leadership.Term` must be **monotonically non-decreasing**, strictly greater on
every leader change — an epoch / fencing token. All methods must be
concurrency-safe and honour `ctx`.

## Implementing a `Fencer`

```go
type Fencer interface { Fence(ctx context.Context, writer string) error }
```

`Fence` must **isolate the named writer so it can no longer issue a single write
to the shared replicas**, and **return nil only once that is definitively true**.
The Controller opens the write gate the moment `Fence` returns nil — so a
`Fencer` that returns nil *without* actually stopping the old writer silently
re-introduces split-brain. When in doubt, return an error: a failed fence keeps
the new leader passive (safe); a falsely-successful one corrupts data.

Back it with whatever your substrate offers, strongest first:

- **Power / STONITH** — hard-stop the old writer's machine or micro-VM (this is
  what weft does, via its agent: `StopVM`, then poll until confirmed-stopped). A
  stopped node cannot write — the gold standard.
- **Cloud control plane** — force-detach the volume / power off the instance via
  the provider API.
- **Fabric isolation** — an IPMI/PDU power cut, or a network ACL that severs the
  old writer from the replicas.
- **Lease / credential revocation** — revoke the token the old writer presents to
  the replicas (only safe if the replicas actually enforce it on every write).

`Fence` should be **idempotent** (fencing an already-dead node returns nil) and
honour `ctx` (a timeout → return an error → the new leader stays passive and
retries on the next observation).

## Known limitation (honest)

If a node loses its lease *during* a slow `Fence` and then activates, there is a
brief window before it processes the demotion. That window is closed by **STONITH
itself** (the new leader hard-stops this node) and, ultimately, by threading a
**fencing token** (the lease `Term`) onto every replica write so a stale writer's
writes are rejected at the replica — the `activate(token)` hook is reserved for
exactly this. This is the standard state of the art (Longhorn and Patroni rely on
the same fencing model); `replica-ha` is explicit that watertightness depends on
the `Fencer` being real.

## Status

Core control plane complete — `Coordinator` seam, `Controller` state machine,
`ActiveDevice` gate — 100% test coverage on all 6 supported 64-bit
architectures. An etcd-backed `Coordinator` (mirroring weft-ha-postgresql's
`EtcdDCS`, with an embedded-etcd integration test) and weft's STONITH `Fencer`
are integrator-side follow-ups that plug into these seams.

## License

BSD-3-Clause © the go-volumes/replica-ha authors.
