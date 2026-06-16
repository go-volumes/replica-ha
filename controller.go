package replicaha

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-volumes/replica"
)

// Role is the Controller's role for a volume at a point in time.
type Role int

const (
	// RoleFollower means this node is not the leader: the write gate is closed
	// and [ActiveDevice.WriteAt] returns [ErrNotLeader].
	RoleFollower Role = iota
	// RoleFencePending means this node has WON leadership but has not yet
	// confirmed the prior writer is fenced. It is NOT writing — this is the
	// safety state that prevents split-brain. It becomes [RoleLeader] only after
	// a successful fence, or falls back to follower behaviour (still not
	// writing) if the fence fails.
	RoleFencePending
	// RoleLeader means this node is the confirmed active writer: it holds the
	// lease AND has fenced the prior writer, so the write gate is open.
	RoleLeader
)

// String renders a Role for logs and status output.
func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleFencePending:
		return "fence-pending"
	case RoleLeader:
		return "leader"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// Status is a snapshot of the Controller's state, returned by
// [Controller.Status].
type Status struct {
	// Role is the current control-plane role.
	Role Role
	// Term is the election term of the current (or most recent) leadership
	// observation.
	Term int64
	// PrevWriter is the node ID this controller fenced (or attempted to fence)
	// on its most recent promotion. Empty if it has never promoted.
	PrevWriter string
	// Active reports whether the write gate is currently open.
	Active bool
	// Replicas is the engine's per-replica replication state
	// ([replica.Engine.Status]).
	Replicas []replica.ReplicaStatus
}

// Controller is the per-node reconcile state machine that keeps exactly one
// node the active writer for a volume. It campaigns for leadership through a
// [Coordinator], fences the prior writer through a [replica.Fencer] BEFORE
// activating its [replica.Engine] as the writer, and demotes immediately when
// leadership is lost. Construct it with [New] and drive it with
// [Controller.Run].
type Controller struct {
	coord  Coordinator
	fencer replica.Fencer
	dev    *ActiveDevice
	engine *replica.Engine
	log    *slog.Logger

	mu         sync.Mutex // guards the fields below
	role       Role
	term       int64
	prevWriter string // last leader we observed that was not us
	lastLeader string // last non-empty leader seen (to fence on promotion)
	stopped    bool

	resignFn func(context.Context) error // = coord.Resign; swappable in tests
}

// New builds a Controller over engine, coord and fencer. It returns the
// Controller and the [ActiveDevice] the data path must write through — that
// device starts PASSIVE and only becomes writable once the Controller has
// promoted (fenced the prior writer). All three dependencies are required.
//
// If log is nil a discarding logger is used.
func New(engine *replica.Engine, coord Coordinator, fencer replica.Fencer, log *slog.Logger) (*Controller, *ActiveDevice, error) {
	if engine == nil {
		return nil, nil, ErrNoEngine
	}
	if coord == nil {
		return nil, nil, ErrNoCoordinator
	}
	if fencer == nil {
		return nil, nil, ErrNoFencer
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	dev := newActiveDevice(engine)
	c := &Controller{
		coord:  coord,
		fencer: fencer,
		dev:    dev,
		engine: engine,
		log:    log,
		role:   RoleFollower,
	}
	c.resignFn = coord.Resign
	return c, dev, nil
}

// Device returns the gated [ActiveDevice] the data path writes through.
func (c *Controller) Device() *ActiveDevice { return c.dev }

// Run drives the control loop until ctx is cancelled or [Controller.Stop] is
// called. The shape:
//
//  1. Subscribe to leadership changes via [Coordinator.Observe].
//  2. Campaign for leadership in the background; when it returns we believe we
//     hold the lease, but we wait for the observed Leadership to confirm us
//     before promoting — the observation is the authoritative signal.
//  3. On each observed Leadership:
//     - IsSelf true  → promote: FENCE the prior writer, then (only on a
//     confirmed fence) open the write gate. A failed/timed-out fence leaves
//     us in [RoleFencePending] (not writing); the next observation retries.
//     - IsSelf false → demote immediately: close the write gate so no further
//     writes are accepted (lost lease / a peer won / partition).
//
// Run returns ctx.Err() on cancellation, or [ErrStopped] after Stop.
func (c *Controller) Run(ctx context.Context) error {
	ch, err := c.coord.Observe(ctx)
	if err != nil {
		return err
	}

	// Campaign in the background. Its return means "lease acquired"; the
	// authoritative promote signal is still the observed Leadership, so we do
	// not act on the campaign's return directly — we just let it unblock the
	// coordinator's election machinery. A campaign error other than
	// cancellation is logged; the loop keeps serving demotions either way.
	campaignDone := make(chan error, 1)
	go func() { campaignDone <- c.coord.Campaign(ctx) }()

	for {
		select {
		case <-ctx.Done():
			c.demote("context cancelled")
			return ctx.Err()
		case err := <-campaignDone:
			// Drain the campaign result so the goroutine cannot leak; a nil
			// result means we won (the promote happens on the observation), a
			// non-cancel error is worth a log line.
			campaignDone = nil // never select this case again
			if err != nil && ctx.Err() == nil {
				c.log.Warn("campaign returned an error", "err", err)
			}
		case lead, ok := <-ch:
			if !ok {
				// The coordinator closed the leadership stream: its session ended
				// (lease lost / store unreachable). We can no longer trust our
				// leadership, so demote and stop. Plain ctx cancellation is
				// handled by the <-ctx.Done() case above, so reaching here always
				// means a session death rather than a clean shutdown.
				c.demote("leadership stream closed")
				return ErrStopped
			}
			c.onLeadership(ctx, lead)
		}
	}
}

// onLeadership applies one observed [Leadership] to the state machine.
func (c *Controller) onLeadership(ctx context.Context, lead Leadership) {
	c.mu.Lock()
	c.term = lead.Term
	// Remember the most recent OTHER leader so that when we are promoted we
	// know whom to fence. An empty leader (no-leader gap) does not overwrite a
	// known prior writer.
	if lead.Leader != "" && !lead.IsSelf {
		c.lastLeader = lead.Leader
	}
	c.mu.Unlock()

	if lead.IsSelf {
		c.promote(ctx)
		return
	}
	c.demote("observed another leader")
}

// promote runs fence-before-promote. It fences the prior writer through the
// [replica.Fencer]; only on a confirmed fence does it open the write gate and
// enter [RoleLeader]. A failed or timed-out fence leaves the controller in
// [RoleFencePending] WITHOUT writing — never promoting into a maybe-live old
// writer (split-brain).
func (c *Controller) promote(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	prev := c.lastLeader
	c.prevWriter = prev
	c.role = RoleFencePending
	c.mu.Unlock()

	// If there is no known prior writer (e.g. this is the cluster's first
	// leader, with no predecessor to fence), there is nothing to STONITH and
	// promotion is immediately safe.
	if prev == "" {
		c.activate(0)
		c.log.Info("promoted (no prior writer to fence)")
		return
	}

	if err := c.fencer.Fence(ctx, prev); err != nil {
		// REFUSE to promote: stay fence-pending, gate closed. The next observed
		// Leadership (or the operator) retries; we never write meanwhile.
		c.log.Error("fence failed — refusing to promote", "prev_writer", prev,
			"err", fmt.Errorf("%w: %w", ErrFenceFailed, err))
		return
	}
	c.activate(0)
	c.log.Info("promoted (prior writer fenced)", "prev_writer", prev)
}

// activate opens the write gate and records [RoleLeader]. extra is unused today
// (reserved so a fencing token can be threaded onto the data path later); it is
// kept zero by callers.
func (c *Controller) activate(_ int64) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.role = RoleLeader
	c.mu.Unlock()
	c.dev.setActive(true)
}

// demote closes the write gate and records [RoleFollower]. It is idempotent: a
// node that is already a follower stays one. reason is logged only on an actual
// transition away from leadership so steady-state follower ticks are quiet.
func (c *Controller) demote(reason string) {
	c.mu.Lock()
	wasActive := c.role != RoleFollower
	c.role = RoleFollower
	c.mu.Unlock()
	c.dev.setActive(false)
	if wasActive {
		c.log.Warn("demoted — write gate closed", "reason", reason)
	}
}

// Status returns a snapshot of the controller's role, term, prior-writer
// identity, write-gate state, and the engine's replica status.
func (c *Controller) Status() Status {
	c.mu.Lock()
	st := Status{
		Role:       c.role,
		Term:       c.term,
		PrevWriter: c.prevWriter,
	}
	c.mu.Unlock()
	st.Active = c.dev.IsActive()
	st.Replicas = c.engine.Status()
	return st
}

// Members returns the currently-live node IDs from the [Coordinator].
func (c *Controller) Members(ctx context.Context) ([]string, error) {
	return c.coord.Members(ctx)
}

// Stop deactivates the write gate and resigns leadership through the
// [Coordinator]. It is safe to call once; subsequent calls are no-ops that
// return nil. Stop does not cancel a running [Controller.Run]; cancel its
// context for that. Stop ensures that after it returns this node is not the
// active writer and has released its lease.
func (c *Controller) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	c.role = RoleFollower
	c.mu.Unlock()

	c.dev.setActive(false)
	if err := c.resignFn(ctx); err != nil {
		return fmt.Errorf("resign on stop: %w", err)
	}
	return nil
}
