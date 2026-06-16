package replicaha

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	volume "github.com/go-volumes/interface"
	"github.com/go-volumes/replica"
)

// --- in-memory volume.Device (mirrors the replica package's test device) ---

type memDevice struct {
	mu   sync.Mutex
	data []byte
}

func newMemDevice(size int) *memDevice { return &memDevice{data: make([]byte, size)} }

func (m *memDevice) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off < 0 || off >= int64(len(m.data)) {
		return 0, errors.New("read out of range")
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *memDevice) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(m.data)) {
		return 0, errors.New("write out of range")
	}
	n := copy(m.data[off:], p)
	return n, nil
}

func (m *memDevice) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *memDevice) Sync() error          { return nil }
func (m *memDevice) Close() error         { return nil }

var _ volume.Device = (*memDevice)(nil)

func newEngine(t *testing.T) *replica.Engine {
	t.Helper()
	eng, err := replica.New([]replica.Replica{
		{Name: "a", Dev: newMemDevice(4096)},
		{Name: "b", Dev: newMemDevice(4096)},
	}, replica.Config{})
	if err != nil {
		t.Fatalf("replica.New: %v", err)
	}
	return eng
}

// --- fake Coordinator: a scripted channel of Leadership events ---

type fakeCoord struct {
	id     string
	ch     chan Leadership
	obsErr error // returned by Observe if non-nil

	mu          sync.Mutex
	members     []string
	membersErr  error
	campaignErr error
	resignErr   error
	resignCalls int
	campaigned  chan struct{} // closed when Campaign is entered
	campaignWB  bool          // if true Campaign blocks until ctx done
}

func newFakeCoord(id string) *fakeCoord {
	return &fakeCoord{
		id:         id,
		ch:         make(chan Leadership, 16),
		members:    []string{id},
		campaigned: make(chan struct{}),
	}
}

func (f *fakeCoord) NodeID() string { return f.id }

func (f *fakeCoord) Campaign(ctx context.Context) error {
	f.mu.Lock()
	select {
	case <-f.campaigned:
	default:
		close(f.campaigned)
	}
	block := f.campaignWB
	cerr := f.campaignErr
	f.mu.Unlock()
	if cerr != nil {
		return cerr
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeCoord) Resign(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resignCalls++
	return f.resignErr
}

func (f *fakeCoord) Observe(ctx context.Context) (<-chan Leadership, error) {
	if f.obsErr != nil {
		return nil, f.obsErr
	}
	// The stream is driven explicitly by emit / close(f.ch) in tests. Plain
	// context cancellation is handled by the Controller's own <-ctx.Done()
	// case, so the fake does not auto-close on ctx.Done.
	return f.ch, nil
}

func (f *fakeCoord) Members(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.members, f.membersErr
}

// emit pushes a leadership event onto the scripted stream.
func (f *fakeCoord) emit(l Leadership) { f.ch <- l }

var _ Coordinator = (*fakeCoord)(nil)

// --- recording / failing Fencer ---

type recordingFencer struct {
	mu      sync.Mutex
	fenced  []string
	err     error // returned by Fence when non-nil
	calls   int
	gate    chan struct{} // if non-nil, Fence blocks on it before returning
	entered chan struct{} // if non-nil, closed when Fence is first entered
}

func (rf *recordingFencer) Fence(ctx context.Context, replicaName string) error {
	rf.mu.Lock()
	rf.calls++
	cerr := rf.err
	gate := rf.gate
	entered := rf.entered
	rf.mu.Unlock()
	if entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	if gate != nil {
		<-gate
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if cerr != nil {
		return cerr
	}
	rf.fenced = append(rf.fenced, replicaName)
	return nil
}

func (rf *recordingFencer) snapshot() (int, []string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	out := append([]string(nil), rf.fenced...)
	return rf.calls, out
}

var _ replica.Fencer = (*recordingFencer)(nil)

// waitFor polls cond until true or the deadline elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within deadline")
}

// runController starts a Controller's Run loop in the background and returns a
// stop func that cancels it and waits for Run to return.
func runController(t *testing.T, c *Controller) (cancel context.CancelFunc, wait func() error) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(ctx) }()
	return cancelFn, func() error { return <-errCh }
}

// --- tests ---

func TestNewValidation(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}

	if _, _, err := New(nil, coord, fn, nil); !errors.Is(err, ErrNoEngine) {
		t.Fatalf("nil engine: got %v", err)
	}
	if _, _, err := New(eng, nil, fn, nil); !errors.Is(err, ErrNoCoordinator) {
		t.Fatalf("nil coordinator: got %v", err)
	}
	if _, _, err := New(eng, coord, nil, nil); !errors.Is(err, ErrNoFencer) {
		t.Fatalf("nil fencer: got %v", err)
	}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Device() != dev {
		t.Fatalf("Device() mismatch")
	}
	if dev.IsActive() {
		t.Fatalf("device should start passive")
	}
}

func TestBecomeLeaderFencesPriorWriterThenActivates(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n2")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	// First, observe that n1 is the leader (records prior writer).
	coord.emit(Leadership{Leader: "n1", IsSelf: false, Term: 1})
	waitFor(t, func() bool { return c.Status().Term == 1 })

	// Writes are rejected while a follower.
	if _, err := dev.WriteAt([]byte("x"), 0); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower write: got %v", err)
	}

	// Now we win leadership: must fence n1 then activate.
	coord.emit(Leadership{Leader: "n2", IsSelf: true, Term: 2})
	waitFor(t, func() bool { return dev.IsActive() })

	calls, fenced := fn.snapshot()
	if calls != 1 || len(fenced) != 1 || fenced[0] != "n1" {
		t.Fatalf("expected to fence n1 once, got calls=%d fenced=%v", calls, fenced)
	}
	st := c.Status()
	if st.Role != RoleLeader || st.Term != 2 || st.PrevWriter != "n1" || !st.Active {
		t.Fatalf("unexpected status: %+v", st)
	}
	if len(st.Replicas) != 2 {
		t.Fatalf("expected 2 replicas in status, got %d", len(st.Replicas))
	}

	// Active-writer data path: WriteAt then ReadAt round-trips through engine.
	if n, err := dev.WriteAt([]byte("hello"), 0); err != nil || n != 5 {
		t.Fatalf("active write: n=%d err=%v", n, err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("active sync: %v", err)
	}
	buf := make([]byte, 5)
	if n, err := dev.ReadAt(buf, 0); err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("read back: n=%d buf=%q err=%v", n, buf[:n], err)
	}
	if sz, err := dev.Size(); err != nil || sz != 4096 {
		t.Fatalf("size: %d err=%v", sz, err)
	}
}

func TestFenceFailureStaysPassiveNoSplitBrain(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n2")
	fn := &recordingFencer{err: errors.New("vm still running")}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "old", IsSelf: false, Term: 1})
	waitFor(t, func() bool { return c.Status().Term == 1 })
	coord.emit(Leadership{Leader: "n2", IsSelf: true, Term: 2})

	// Fence is attempted but fails → must remain fence-pending and PASSIVE.
	waitFor(t, func() bool { return c.Status().Role == RoleFencePending })
	if dev.IsActive() {
		t.Fatalf("must NOT activate after a failed fence (split-brain risk)")
	}
	if _, err := dev.WriteAt([]byte("x"), 0); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write must be rejected while fence-pending: %v", err)
	}
	if calls, _ := fn.snapshot(); calls != 1 {
		t.Fatalf("expected one fence attempt, got %d", calls)
	}
}

func TestFirstLeaderNoPriorWriterActivatesWithoutFencing(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("solo")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	// We win with no prior leader ever observed → nothing to fence.
	coord.emit(Leadership{Leader: "solo", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })
	if calls, _ := fn.snapshot(); calls != 0 {
		t.Fatalf("expected no fence with no prior writer, got %d", calls)
	}
	if c.Status().Role != RoleLeader {
		t.Fatalf("expected leader role")
	}
}

func TestLoseLeadershipDeactivates(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })
	if _, err := dev.WriteAt([]byte("ok"), 0); err != nil {
		t.Fatalf("leader write should succeed: %v", err)
	}

	// Lost lease: another node observed as leader.
	coord.emit(Leadership{Leader: "n9", IsSelf: false, Term: 2})
	waitFor(t, func() bool { return !dev.IsActive() })

	if _, err := dev.WriteAt([]byte("no"), 0); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write must be rejected after losing leadership: %v", err)
	}
	if err := dev.Sync(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("sync must be rejected after losing leadership: %v", err)
	}
	if c.Status().Role != RoleFollower {
		t.Fatalf("expected follower role")
	}
	// The new prior writer (n9) is recorded for a future promotion.
	if c.Status().Term != 2 {
		t.Fatalf("expected term 2")
	}
}

func TestLostLeaseEmptyLeaderDeactivates(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })

	// Lease expired: no leader at all (empty Leader, IsSelf false).
	coord.emit(Leadership{Leader: "", IsSelf: false, Term: 2})
	waitFor(t, func() bool { return !dev.IsActive() })
	if c.Status().Role != RoleFollower {
		t.Fatalf("expected follower after lost lease")
	}
}

func TestMembers(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	coord.members = []string{"n1", "n2", "n3"}
	fn := &recordingFencer{}
	c, _, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := c.Members(context.Background())
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 members, got %v", got)
	}

	coord.membersErr = errors.New("etcd down")
	if _, err := c.Members(context.Background()); err == nil {
		t.Fatalf("expected members error")
	}
}

func TestStopResignsAndDeactivates(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if dev.IsActive() {
		t.Fatalf("Stop must deactivate the write gate")
	}
	coord.mu.Lock()
	rc := coord.resignCalls
	coord.mu.Unlock()
	if rc != 1 {
		t.Fatalf("expected one resign, got %d", rc)
	}
	// Idempotent.
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	coord.mu.Lock()
	rc = coord.resignCalls
	coord.mu.Unlock()
	if rc != 1 {
		t.Fatalf("second Stop must be a no-op, resignCalls=%d", rc)
	}

	// A promote after Stop must not activate (stopped guard in promote/activate).
	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 3})
	time.Sleep(20 * time.Millisecond)
	if dev.IsActive() {
		t.Fatalf("must not activate after Stop")
	}
}

func TestStopResignError(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	coord.resignErr = errors.New("resign boom")
	fn := &recordingFencer{}
	c, _, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Stop(context.Background()); err == nil {
		t.Fatalf("expected resign error from Stop")
	}
}

func TestObserveError(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	coord.obsErr = errors.New("cannot subscribe")
	fn := &recordingFencer{}
	c, _, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Run(context.Background()); err == nil {
		t.Fatalf("expected Run to return the Observe error")
	}
}

func TestContextCancellationDemotesAndReturns(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(ctx) }()

	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })

	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if dev.IsActive() {
		t.Fatalf("cancellation must deactivate the write gate")
	}
}

func TestLeadershipStreamClosedStops(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	fn := &recordingFencer{}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(context.Background()) }()

	coord.emit(Leadership{Leader: "n1", IsSelf: true, Term: 1})
	waitFor(t, func() bool { return dev.IsActive() })

	// Close the stream while ctx is still live → Run returns ErrStopped.
	close(coord.ch)
	if err := <-errCh; !errors.Is(err, ErrStopped) {
		t.Fatalf("expected ErrStopped, got %v", err)
	}
	if dev.IsActive() {
		t.Fatalf("stream close must deactivate")
	}
}

func TestCampaignErrorLogged(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	coord.campaignErr = errors.New("campaign boom")
	fn := &recordingFencer{}
	c, _, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(ctx) }()
	// Campaign returns an error promptly; give the loop a tick to drain it.
	waitFor(t, func() bool {
		select {
		case <-coord.campaigned:
			return true
		default:
			return false
		}
	})
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestCampaignBlocksUntilCancel(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n1")
	coord.campaignWB = true // Campaign blocks until ctx done
	fn := &recordingFencer{}
	c, _, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(ctx) }()
	waitFor(t, func() bool {
		select {
		case <-coord.campaigned:
			return true
		default:
			return false
		}
	})
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestDeviceClose(t *testing.T) {
	eng := newEngine(t)
	dev := newActiveDevice(eng)
	if err := dev.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStopDuringFenceDoesNotActivate exercises the stopped re-check inside
// activate: Stop lands while a slow fence is in flight, so when the fence
// returns nil the controller must NOT open the write gate.
func TestStopDuringFenceDoesNotActivate(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n2")
	fn := &recordingFencer{
		gate:    make(chan struct{}),
		entered: make(chan struct{}),
	}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "old", IsSelf: false, Term: 1})
	waitFor(t, func() bool { return c.Status().Term == 1 })
	coord.emit(Leadership{Leader: "n2", IsSelf: true, Term: 2})

	// Wait until the fence is in flight, then Stop, then release the fence.
	<-fn.entered
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	close(fn.gate) // fence returns nil → activate runs but must see stopped
	time.Sleep(20 * time.Millisecond)
	if dev.IsActive() {
		t.Fatalf("activate must not open the gate after Stop")
	}
}

func TestRoleString(t *testing.T) {
	cases := map[Role]string{
		RoleFollower:     "follower",
		RoleFencePending: "fence-pending",
		RoleLeader:       "leader",
		Role(99):         "Role(99)",
	}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Fatalf("Role(%d).String() = %q, want %q", int(r), got, want)
		}
	}
}

// TestRepeatedPromotionWhilePending exercises a promote arriving while already
// fence-pending: the fence retries on each observation.
func TestPromoteRetriesAfterFenceRecovers(t *testing.T) {
	eng := newEngine(t)
	coord := newFakeCoord("n2")
	fn := &recordingFencer{err: errors.New("not yet")}
	c, dev, err := New(eng, coord, fn, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cancel, wait := runController(t, c)
	defer func() { cancel(); _ = wait() }()

	coord.emit(Leadership{Leader: "old", IsSelf: false, Term: 1})
	waitFor(t, func() bool { return c.Status().Term == 1 })
	coord.emit(Leadership{Leader: "n2", IsSelf: true, Term: 2})
	waitFor(t, func() bool {
		calls, _ := fn.snapshot()
		return calls == 1
	})
	if dev.IsActive() {
		t.Fatalf("must stay passive while fence fails")
	}

	// Fence now recovers; a re-observation promotes successfully.
	fn.mu.Lock()
	fn.err = nil
	fn.mu.Unlock()
	coord.emit(Leadership{Leader: "n2", IsSelf: true, Term: 2})
	waitFor(t, func() bool { return dev.IsActive() })
	if c.Status().Role != RoleLeader {
		t.Fatalf("expected leader after fence recovers")
	}
}
