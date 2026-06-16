package replicaha

import "errors"

var (
	// ErrNotLeader is returned by the write path ([ActiveDevice.WriteAt],
	// [ActiveDevice.Sync]) when the [Controller] is not the active writer — it
	// is a follower, or a freshly-elected leader that has not yet confirmed the
	// prior writer is fenced. Reads are always permitted; only the write path is
	// gated.
	ErrNotLeader = errors.New("replica-ha: not the active writer (not leader)")

	// ErrFenceFailed is returned (and logged) when fence-before-promote could
	// not confirm the prior writer is fenced. On this error the Controller
	// refuses to activate and stays passive: it never promotes into a
	// maybe-live old writer. It wraps the underlying [replica.Fencer] error.
	ErrFenceFailed = errors.New("replica-ha: fence failed — refusing to promote")

	// ErrStopped is returned by [Controller.Run] when the controller has been
	// stopped via [Controller.Stop], and by operations attempted after Stop.
	ErrStopped = errors.New("replica-ha: controller stopped")

	// ErrNoEngine is returned by [New] when no [replica.Engine] is supplied.
	ErrNoEngine = errors.New("replica-ha: nil engine")

	// ErrNoCoordinator is returned by [New] when no [Coordinator] is supplied.
	ErrNoCoordinator = errors.New("replica-ha: nil coordinator")

	// ErrNoFencer is returned by [New] when no [replica.Fencer] is supplied. A
	// control plane with no way to fence cannot promote safely, so a nil fencer
	// is rejected at construction rather than discovered at the first failover.
	ErrNoFencer = errors.New("replica-ha: nil fencer")
)
