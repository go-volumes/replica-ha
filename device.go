package replicaha

import (
	"sync/atomic"

	volume "github.com/go-volumes/interface"
	"github.com/go-volumes/replica"
)

// ActiveDevice wraps a [replica.Engine] with an active/passive write gate. It
// is how this package models "active writer": the engine itself has no notion
// of leadership, so the gate lives in front of it. While the owning
// [Controller] is the active writer the gate is open and every call passes
// straight through to the engine; while it is passive the WRITE path
// ([ActiveDevice.WriteAt], [ActiveDevice.Sync]) returns [ErrNotLeader] without
// touching any replica, and the READ path ([ActiveDevice.ReadAt],
// [ActiveDevice.Size]) is always allowed (a follower may serve reads).
//
// An ActiveDevice satisfies [volume.Device], so a filesystem-format driver or
// an NBD server can write its on-disk image through it unchanged and will be
// transparently rejected with [ErrNotLeader] the instant this node stops being
// the leader — the data path needs no awareness of leadership beyond handling
// that error.
//
// The gate is a single atomic bool: flipping it is wait-free and safe to call
// from the Controller's run loop while writes are in flight on other
// goroutines. A write that observes "active" and then races a demotion is the
// normal in-flight-write case; the lease's grace window plus the prior-writer
// fence on the NEXT promotion are what bound safety, not per-write locking.
type ActiveDevice struct {
	engine *replica.Engine
	active atomic.Bool
}

// Compile-time assertion that *ActiveDevice satisfies the volume contract.
var _ volume.Device = (*ActiveDevice)(nil)

// newActiveDevice wraps engine starting in the PASSIVE state (writes rejected).
// Only a [Controller] that has fenced the prior writer flips it active.
func newActiveDevice(engine *replica.Engine) *ActiveDevice {
	return &ActiveDevice{engine: engine}
}

// IsActive reports whether the write gate is currently open.
func (d *ActiveDevice) IsActive() bool { return d.active.Load() }

// setActive opens (true) or closes (false) the write gate.
func (d *ActiveDevice) setActive(active bool) { d.active.Store(active) }

// WriteAt mirrors p at off to the engine when this node is the active writer,
// and returns (0, [ErrNotLeader]) otherwise without touching any replica.
func (d *ActiveDevice) WriteAt(p []byte, off int64) (int, error) {
	if !d.active.Load() {
		return 0, ErrNotLeader
	}
	return d.engine.WriteAt(p, off)
}

// Sync flushes the engine when active, and returns [ErrNotLeader] otherwise.
func (d *ActiveDevice) Sync() error {
	if !d.active.Load() {
		return ErrNotLeader
	}
	return d.engine.Sync()
}

// ReadAt always passes through to the engine: a follower may serve reads.
func (d *ActiveDevice) ReadAt(p []byte, off int64) (int, error) {
	return d.engine.ReadAt(p, off)
}

// Size reports the replicated volume size; always allowed.
func (d *ActiveDevice) Size() (int64, error) { return d.engine.Size() }

// Close releases the underlying engine. After Close the device must not be
// used again.
func (d *ActiveDevice) Close() error { return d.engine.Close() }
