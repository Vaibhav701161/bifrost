// Package utils — stream-timeout observability + bounded-goroutine controls.
//
// Production-hardening additions on top of the foundational StreamController
// API in streamcontroller.go and the idle/total timeout helpers in utils.go:
//
//   1. Goroutine accounting — atomic counters for active stream-cancellation
//      goroutines + active per-Read goroutines. Exposed via package functions
//      so observability plugins or pprof handlers can read live values.
//   2. Hard cap — env-tunable upper bound on simultaneous timeout goroutines.
//      When exceeded, NEW timeout wrappers are REFUSED with ErrStreamTimeoutCapExceeded.
//      Enforced at admission via TryAcquireStreamTimeoutSlot so callers
//      degrade gracefully (stream still proceeds without timeout enforcement,
//      with a structured WARN). No unbounded goroutine growth is possible.
//   3. Structured timeout-event logger — single hook used by every fired-path
//      with truthful semantics:
//        - close_invoked      : did we call the close function?
//        - close_result       : ok | err:<msg> | nil-controller
//        - termination_guarantee : best_effort
//
// Why "best_effort" not "terminated": *fasthttp.closeReader.CloseBodyStream
// flips a flag and the next Read OBSERVES the flag — but a Read already
// parked in net.Conn.Read remains parked until kernel TCP close / RST /
// SO_RCVTIMEO. We surface this honestly rather than claim guarantees the
// transport cannot deliver.
package utils

import (
"errors"
"fmt"
"os"
"strconv"
"sync/atomic"
"time"
)

// ---------------------------------------------------------------------------
// Goroutine accounting + soft cap.
// ---------------------------------------------------------------------------

// ErrStreamTimeoutCapExceeded is returned by TryAcquireStreamTimeoutSlot
// when active timeout goroutines have hit the configured hard cap. Callers
// MUST handle this — typically by logging and proceeding without timeout
// enforcement (graceful degradation) rather than failing the whole stream.
var ErrStreamTimeoutCapExceeded = errors.New("bifrost: stream timeout goroutine cap exceeded")

const (
	// envMaxStreamTimeoutGoroutines tunes the soft cap. 0 / unset → 4096.
	envMaxStreamTimeoutGoroutines = "BIFROST_MAX_STREAM_TIMEOUT_GOROUTINES"
	defaultMaxStreamTimeoutGoroutines int64 = 4096
)

var (
	// activeStreamCancellationGoroutines counts SetupStreamCancellation watchers
	// that have not yet exited (one per active stream).
	activeStreamCancellationGoroutines atomic.Int64

	// activeStreamReadGoroutines counts inner-Read goroutines spawned by
	// idleTimeoutReader.Read. Should typically equal the number of streams
	// currently blocked in Read (i.e. close to active streams, not multiples).
	activeStreamReadGoroutines atomic.Int64

	// totalStreamIdleTimeoutsFired / totalStreamTotalTimeoutsFired count
	// lifetime fired events. Useful for prometheus pull / debug endpoints.
	totalStreamIdleTimeoutsFired  atomic.Int64
	totalStreamTotalTimeoutsFired atomic.Int64
	totalStreamCtxCancelsFired    atomic.Int64

	// totalStreamCloseFnInvocations counts each successful at-most-once
	// closeFn dispatch (from any of the three trigger paths).
	totalStreamCloseFnInvocations atomic.Int64

	// maxStreamTimeoutGoroutines caches the parsed env value. Loaded once;
	// subsequent reads via maxStreamTimeoutGoroutinesValue() snapshot atomic.
	maxStreamTimeoutGoroutines atomic.Int64
)

func init() {
	maxStreamTimeoutGoroutines.Store(parseMaxStreamTimeoutGoroutines())
}

func parseMaxStreamTimeoutGoroutines() int64 {
	v, _ := strconv.ParseInt(os.Getenv(envMaxStreamTimeoutGoroutines), 10, 64)
	if v <= 0 {
		return defaultMaxStreamTimeoutGoroutines
	}
	return v
}

// MaxStreamTimeoutGoroutines returns the configured soft cap. Operators can
// raise it if they expect very high streaming concurrency.
func MaxStreamTimeoutGoroutines() int64 { return maxStreamTimeoutGoroutines.Load() }

// SetMaxStreamTimeoutGoroutines is exposed for tests and admin handlers.
func SetMaxStreamTimeoutGoroutines(n int64) {
	if n <= 0 {
		n = defaultMaxStreamTimeoutGoroutines
	}
	maxStreamTimeoutGoroutines.Store(n)
}

// StreamTimeoutMetricsSnapshot is a lightweight read-only view of the live
// goroutine / fired counters. Returned by StreamTimeoutMetrics.
type StreamTimeoutMetricsSnapshot struct {
	ActiveCancellationGoroutines int64 `json:"active_cancellation_goroutines"`
	ActiveReadGoroutines         int64 `json:"active_read_goroutines"`
	TotalIdleTimeoutsFired       int64 `json:"total_idle_timeouts_fired"`
	TotalTotalTimeoutsFired      int64 `json:"total_total_timeouts_fired"`
	TotalCtxCancelsFired         int64 `json:"total_ctx_cancels_fired"`
	TotalCloseFnInvocations      int64 `json:"total_closefn_invocations"`
	TotalAdmissionsRejected      int64 `json:"total_admissions_rejected"`
	HardCapGoroutines            int64 `json:"hard_cap_goroutines"`
}

// StreamTimeoutMetrics returns a snapshot of the current goroutine + fired
// counters. Safe to call concurrently. Useful from /debug/pprof and from
// observability plugins.
func StreamTimeoutMetrics() StreamTimeoutMetricsSnapshot {
	return StreamTimeoutMetricsSnapshot{
		ActiveCancellationGoroutines: activeStreamCancellationGoroutines.Load(),
		ActiveReadGoroutines:         activeStreamReadGoroutines.Load(),
		TotalIdleTimeoutsFired:       totalStreamIdleTimeoutsFired.Load(),
		TotalTotalTimeoutsFired:      totalStreamTotalTimeoutsFired.Load(),
		TotalCtxCancelsFired:         totalStreamCtxCancelsFired.Load(),
		TotalCloseFnInvocations:      totalStreamCloseFnInvocations.Load(),
		TotalAdmissionsRejected:      totalStreamTimeoutAdmissionsRejected.Load(),
		HardCapGoroutines:            maxStreamTimeoutGoroutines.Load(),
	}
}

// totalStreamTimeoutAdmissionsRejected counts the number of times the hard
// cap rejected a new timeout slot. Useful operational metric.
var totalStreamTimeoutAdmissionsRejected atomic.Int64

// TryAcquireStreamTimeoutSlot atomically reserves a goroutine slot under the
// hard cap. Returns ErrStreamTimeoutCapExceeded if the cap would be exceeded.
//
// On success the caller MUST call ReleaseStreamTimeoutSlot exactly once when
// the goroutine exits. On failure the caller must NOT spawn the goroutine.
//
// This is the only admission path for stream-cancellation goroutines —
// SetupStreamCancellation calls it internally. ApplyStreamTimeouts uses the
// same accounting via NewIdleTimeoutReader's internal Read goroutine, which
// is gated by activeStreamReadGoroutines (one-per-stream by construction).
func TryAcquireStreamTimeoutSlot() error {
	capVal := maxStreamTimeoutGoroutines.Load()
	if capVal <= 0 {
		activeStreamCancellationGoroutines.Add(1)
		return nil
	}
	for {
		current := activeStreamCancellationGoroutines.Load()
		if current >= capVal {
			n := totalStreamTimeoutAdmissionsRejected.Add(1)
			// Log on every rejection BUT only the first 100 per process to
			// avoid log-flood under sustained saturation; absolute counter
			// remains accurate via StreamTimeoutMetrics().
			if n <= 100 {
				getLogger().Warn(fmt.Sprintf(
					`{"event":"stream.timeout.admission_rejected","active":%d,"cap":%d,"total_rejected":%d,"action":"caller-degrades-without-timeout-enforcement"}`,
					current, capVal, n,
				))
			}
			return ErrStreamTimeoutCapExceeded
		}
		if activeStreamCancellationGoroutines.CompareAndSwap(current, current+1) {
			return nil
		}
	}
}

// ReleaseStreamTimeoutSlot releases a slot acquired via TryAcquireStreamTimeoutSlot.
func ReleaseStreamTimeoutSlot() { activeStreamCancellationGoroutines.Add(-1) }

// noteStreamCancellationStarted is retained as a thin wrapper for callers
// that have already passed admission via TryAcquireStreamTimeoutSlot.
// Direct callers (none in production code) are deprecated; admission MUST
// go through TryAcquireStreamTimeoutSlot.
func noteStreamCancellationStopped() { ReleaseStreamTimeoutSlot() }

func noteStreamReadStarted() { activeStreamReadGoroutines.Add(1) }
func noteStreamReadStopped() { activeStreamReadGoroutines.Add(-1) }

// ---------------------------------------------------------------------------
// Structured timeout event logging.
// ---------------------------------------------------------------------------

// streamTimeoutEvent is the canonical fired-path log shape. Emitted exactly
// once per fired trigger (idle / total / ctx-cancel) by the helpers.
//
// Format is JSON-in-message so existing structured logger backends (zerolog,
// zap, etc.) parse it. We intentionally do not introduce a new logger
// interface here — the existing schemas.Logger is sufficient.
type streamTimeoutEventReason string

const (
	streamTimeoutReasonIdle      streamTimeoutEventReason = "idle"
	streamTimeoutReasonTotal     streamTimeoutEventReason = "total"
	streamTimeoutReasonCtxCancel streamTimeoutEventReason = "ctx_cancel"
)

// emitStreamTimeoutEvent records and logs a fired stream-timeout with
// truthful, unambiguous semantics:
//
//   - close_invoked         : did we actually call the controller's Close?
//   - close_result          : ok | err:<msg> | nil-controller (close_invoked=false)
//   - termination_guarantee : best_effort
//
// We deliberately do NOT log close=ok in a way that implies the upstream
// Read returned synchronously. fasthttp's CloseBodyStream flips a flag; the
// kernel TCP read is unblocked by the OS when the peer flushes / RSTs /
// SO_RCVTIMEO fires. From the user's Read perspective our wrapper returns
// the timeout sentinel immediately (via the goroutine + select pattern),
// but the inner goroutine may live a few extra ms-to-RTT until the kernel
// closes. Calling that "terminated" would be a lie.
func emitStreamTimeoutEvent(reason streamTimeoutEventReason, configured time.Duration, elapsed time.Duration, closeFnNil bool, closeFnErr error) {
	switch reason {
	case streamTimeoutReasonIdle:
		totalStreamIdleTimeoutsFired.Add(1)
	case streamTimeoutReasonTotal:
		totalStreamTotalTimeoutsFired.Add(1)
	case streamTimeoutReasonCtxCancel:
		totalStreamCtxCancelsFired.Add(1)
	}
	closeInvoked := !closeFnNil
	if closeInvoked && closeFnErr == nil {
		totalStreamCloseFnInvocations.Add(1)
	}

	closeResult := "ok"
	switch {
	case closeFnNil:
		closeResult = "nil-controller"
	case closeFnErr != nil:
		closeResult = "err:" + closeFnErr.Error()
	}

	getLogger().Warn(fmt.Sprintf(
		`{"event":"stream.timeout.fired","reason":%q,"configured_ms":%d,"elapsed_ms":%d,"close_invoked":%t,"close_result":%q,"termination_guarantee":"best_effort"}`,
		reason,
		configured.Milliseconds(),
		elapsed.Milliseconds(),
		closeInvoked,
		closeResult,
	))
}
