// Package replicaha is the high-availability control plane for go-volumes
// replicated block volumes. It drives a [replica.Engine] (the data plane that
// mirrors a volume across N synchronous replicas) under leader election so that
// exactly one node is the active writer for a volume at any instant — the safe
// single-active-writer guarantee that makes failover non-destructive.
//
// # The safety property
//
// A replicated volume is only safe if at most one writer is live at a time. If
// two nodes both believe they own the volume (split-brain — the classic
// outcome of a network partition where a stale leader keeps writing while a new
// leader is elected on the other side), they will mirror divergent writes into
// the same replicas and corrupt the volume irrecoverably. This package
// prevents that with fence-before-promote (STONITH): a node that wins
// leadership MUST fence the previous writer — prove it can no longer write —
// BEFORE it activates its own engine as the writer. If the fence fails or times
// out, the node refuses to activate and stays passive. The control plane never
// writes while a possibly-live old writer might also be writing.
//
// # Pieces
//
//   - [Coordinator] is the coordination seam: lease-based leader election and
//     membership. It is an interface so this core is dependency-free and fully
//     testable with an in-memory fake; an etcd-backed implementation (mirroring
//     weft-ha-postgresql's EtcdDCS) is a deferred follow-up that lives outside
//     this package so the safety-critical state machine carries no etcd weight.
//   - [Controller] is the reconcile state machine. It campaigns for
//     leadership, fences the prior writer before promoting, gates the engine
//     between ACTIVE (writable) and PASSIVE (writes rejected), and demotes
//     immediately when leadership is lost.
//   - [ActiveDevice] wraps the [replica.Engine] so that WriteAt/Sync return
//     [ErrNotLeader] whenever the Controller is not the active writer. This is
//     how "active writer" is modelled: the engine itself has no active/passive
//     gate, so the gate lives here, in front of it, on the data path.
//
// The control plane USES the engine and a [replica.Fencer]; it does not
// reimplement replication.
package replicaha
