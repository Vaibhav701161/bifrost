// Package utils — StreamController API for streaming timeout enforcement.
//
// This file defines the StreamController interface and its standard
// constructors (FastHTTPStreamController, NetHTTPStreamController,
// NewStreamController). It is the foundational abstraction every provider
// uses when handing a streaming response to the timeout helpers.
//
// Why it exists: hides whether the underlying transport is fasthttp
// (resp.CloseBodyStream) or net/http (resp.Body.Close) so the close path
// can evolve without touching every provider.
package utils

import (
	"errors"
	"fmt"
	"io"

	"github.com/valyala/fasthttp"
)

// ---------------------------------------------------------------------------
// StreamController — forward-compatible Close abstraction.
// ---------------------------------------------------------------------------

// StreamController is the contract every provider must satisfy when handing a
// streaming response to the timeout helpers. It hides whether the underlying
// transport is fasthttp (resp.CloseBodyStream) or net/http (resp.Body.Close)
// and lets us evolve the close path without touching providers.
//
// Implementations MUST be safe to invoke from any goroutine. The helpers
// guarantee Close is called at most once via sync.Once but a defensive
// implementation should still tolerate concurrent / repeated calls.
type StreamController interface {
	// Close terminates the upstream stream and unblocks any pending Read.
	// Returns the underlying Close error verbatim (helpers log it but do not
	// propagate to the caller).
	Close() error
}

// streamControllerFunc adapts a closeFn into a StreamController.
type streamControllerFunc struct {
	fn    StreamCloseFunc
	label string
}

func (s streamControllerFunc) Close() error {
	if s.fn == nil {
		return ErrNilStreamController
	}
	return s.fn()
}

// ErrNilStreamController is returned by helpers when closeFn is nil. Reaches
// the caller via ApplyStreamTimeouts so the provider knows enforcement is
// disabled and can decide whether to proceed.
var ErrNilStreamController = errors.New("bifrost: nil stream controller (closeFn missing)")

// FastHTTPStreamController returns a StreamController backed by fasthttp's
// CloseBodyStream. Returns ErrNilStreamController-bearing controller if resp
// is nil (so the helpers surface the misuse via structured logs and the
// strict-contract path in ApplyStreamTimeouts).
//
// This is the canonical constructor for fasthttp-based providers. Providers
// MUST NOT manually wire `func() error { return resp.CloseBodyStream() }`;
// using this helper guarantees uniform error semantics and lets us evolve
// the close path (e.g. add metrics, tracing) in one place.
func FastHTTPStreamController(resp *fasthttp.Response) StreamController {
	if resp == nil {
		return streamControllerFunc{fn: nil, label: "fasthttp:nil-response"}
	}
	return streamControllerFunc{
		fn:    func() error { return resp.CloseBodyStream() },
		label: "fasthttp",
	}
}

// NetHTTPStreamController returns a StreamController backed by net/http's
// Body.Close. Use for providers built on standard net/http (e.g. Bedrock).
func NetHTTPStreamController(body io.Closer) StreamController {
	if body == nil {
		return streamControllerFunc{fn: nil, label: "net/http:nil-body"}
	}
	return streamControllerFunc{
		fn:    body.Close,
		label: "net/http",
	}
}

// NewStreamController wraps an arbitrary close function (test fakes, custom
// transports). A nil fn is a programming bug; this constructor logs a
// structured WARN at construction time so the misconfiguration is visible
// even if the stream never reaches a fired path. Helpers using a nil-fn
// controller will return ErrNilStreamController from their cleanup.
func NewStreamController(fn StreamCloseFunc, debugLabel string) StreamController {
	if fn == nil {
		getLogger().Warn(fmt.Sprintf(
			`{"event":"stream.controller.nil","label":%q,"impact":"timeouts disabled; upstream Read cannot be force-terminated"}`,
			debugLabel,
		))
	}
	return streamControllerFunc{fn: fn, label: debugLabel}
}

// asCloseFn pulls the StreamCloseFunc out of any StreamController. Used by
// the existing helpers (NewIdleTimeoutReader, NewTotalTimeoutReader,
// SetupStreamCancellation, ApplyStreamTimeouts) to keep their internal
// signatures unchanged while letting callers pass a controller.
func asCloseFn(c StreamController) StreamCloseFunc {
	if c == nil {
		return nil
	}
	if scf, ok := c.(streamControllerFunc); ok {
		return scf.fn
	}
	return c.Close
}

// controllerLabel extracts the label set by the typed constructors above.
// Returns "unknown" for foreign StreamController implementations.
func controllerLabel(c StreamController) string {
	if scf, ok := c.(streamControllerFunc); ok && scf.label != "" {
		return scf.label
	}
	return "unknown"
}

