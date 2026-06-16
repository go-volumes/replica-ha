package replicaha

import "context"

// Leadership is a single observation of who currently holds the write lease for
// a volume. It is streamed by [Coordinator.Observe] on every leadership change.
type Leadership struct {
	// Leader is the node ID of the current lease holder. It is empty when no
	// node holds the lease (e.g. during an election after a leader's lease
	// expired). An empty Leader with IsSelf false means "no leader right now".
	Leader string
	// IsSelf is true when Leader is this node ([Coordinator.NodeID]). When it
	// becomes false after having been true, this node has LOST leadership and
	// the [Controller] must stop writing immediately.
	IsSelf bool
	// Term is the monotonically non-decreasing election term (a.k.a. epoch or
	// fencing token). Every successful campaign that changes the leader yields a
	// strictly greater Term. The Controller records it so a new leader can be
	// distinguished from a stale duplicate observation, and so a fencing token
	// could be threaded to the data plane in a future revision.
	Term int64
}

// Coordinator is the coordination seam between the [Controller] and a
// lease-based consensus store (etcd, in the deferred production
// implementation). Keeping it an interface is what lets the safety-critical
// state machine in [Controller] stay dependency-free and exhaustively testable
// with an in-memory fake.
//
// # Lease semantics
//
// Leadership is backed by a time-bounded lease that the implementation renews
// in the background. The lease is the safety hinge of the whole design:
//
//   - While this node holds the lease it is the leader and MAY write (once it
//     has fenced the prior writer — see [Controller]).
//   - If the node stops renewing the lease for any reason — process stall,
//     network partition, the node being fenced by a peer — the lease EXPIRES at
//     the store and leadership is lost. A lost lease MUST surface on the
//     [Coordinator.Observe] channel as a [Leadership] whose IsSelf is false, so
//     the Controller demotes and stops writing before the lease's grace window
//     elapses. An implementation that cannot observe its own lease loss is
//     unsafe and unusable here.
//   - Because the lease expires automatically, a partitioned-away old leader
//     loses its claim without cooperating; the new leader still fences it (it
//     cannot assume the old leader has already noticed) but the lease bounds the
//     window of double ownership.
//
// All methods must be safe for concurrent use and must honour ctx cancellation.
type Coordinator interface {
	// Campaign blocks until this node becomes the leader (acquires and holds the
	// write lease) or ctx is done. It returns nil once leadership is acquired,
	// or ctx.Err() if ctx is cancelled first. It is not an error to call
	// Campaign while already leader; implementations may return immediately.
	Campaign(ctx context.Context) error

	// Resign voluntarily gives up leadership (releases the lease), allowing a
	// peer to win the next election. Resigning when not leader is a no-op and
	// returns nil.
	Resign(ctx context.Context) error

	// Observe returns a channel that streams a [Leadership] on every leadership
	// change for as long as ctx is live. The channel is closed when ctx is done
	// (or the underlying session ends). A lost lease (expiry / partition) MUST
	// be reported here as a Leadership with IsSelf == false so the Controller
	// stops writing. Implementations should deliver the current leadership
	// promptly after the call so a late subscriber is not left blind.
	Observe(ctx context.Context) (<-chan Leadership, error)

	// Members lists the node IDs that currently hold a live membership lease.
	// A fenced or partitioned node drops out automatically as its lease expires,
	// so the returned set reflects only reachable, live peers.
	Members(ctx context.Context) ([]string, error)

	// NodeID is this node's stable identity within the cluster. It matches the
	// Leader field of a [Leadership] when this node is the leader, and the
	// identity a peer passes to the [replica.Fencer] to fence this node.
	NodeID() string
}
