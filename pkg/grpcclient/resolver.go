package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"time"

	"google.golang.org/grpc/resolver"
)

// headlessScheme selects the resolver below in a dial target.
const headlessScheme = "headless"

// lookupHost and lookupInterval are replaced in tests.
var (
	lookupHost     = net.DefaultResolver.LookupHost
	lookupInterval = 5 * time.Second
)

// headlessBuilder resolves a headless Service to its pod IPs and keeps looking
// them up. grpc-go's dns resolver only looks again when a connection breaks and
// at most every 30 seconds, so a new replica would get no traffic until then.
type headlessBuilder struct {
	lookup   func(ctx context.Context, host string) ([]string, error)
	interval time.Duration
}

func newHeadlessBuilder() *headlessBuilder {
	return &headlessBuilder{lookup: lookupHost, interval: lookupInterval}
}

func (b *headlessBuilder) Scheme() string { return headlessScheme }

func (b *headlessBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	_ resolver.BuildOptions,
) (resolver.Resolver, error) {
	host, port, err := net.SplitHostPort(target.Endpoint())
	if err != nil {
		return nil, fmt.Errorf("grpcclient: target %q: %w", target.Endpoint(), err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &headlessResolver{
		lookup:   b.lookup,
		interval: b.interval,
		// The trailing dot makes the name absolute. Without it, ndots:5 in a pod's
		// resolv.conf sends every lookup through all the search domains first.
		name:   host + ".",
		port:   port,
		cc:     cc,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go r.run(ctx)
	return r, nil
}

type headlessResolver struct {
	lookup   func(ctx context.Context, host string) ([]string, error)
	interval time.Duration
	name     string
	port     string
	cc       resolver.ClientConn
	cancel   context.CancelFunc
	done     chan struct{}

	// Only run touches these.
	current []string
	failing bool
}

func (r *headlessResolver) run(ctx context.Context) {
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// refresh hands a changed address list to gRPC. A failed lookup keeps the list
// it has: a DNS problem must not take down connections that still work.
func (r *headlessResolver) refresh(ctx context.Context) {
	lookupCtx, cancel := context.WithTimeout(ctx, r.interval)
	defer cancel()

	hosts, err := r.lookup(lookupCtx, r.name)
	if err == nil && len(hosts) == 0 {
		err = errors.New("no addresses")
	}
	if err != nil {
		switch {
		case ctx.Err() != nil: // closed
		case r.current == nil:
			r.cc.ReportError(fmt.Errorf("grpcclient: look up %s: %w", r.name, err))
		case !r.failing:
			r.failing = true
			slog.Warn("grpcclient: lookup failed, keeping the last addresses", "name", r.name, "error", err)
		}
		return
	}
	if r.failing {
		r.failing = false
		slog.Info("grpcclient: lookup works again", "name", r.name)
	}

	slices.Sort(hosts)
	if slices.Equal(hosts, r.current) {
		return
	}
	r.current = hosts

	endpoints := make([]resolver.Endpoint, 0, len(hosts))
	for _, host := range hosts {
		endpoints = append(endpoints, resolver.Endpoint{
			Addresses: []resolver.Address{{Addr: net.JoinHostPort(host, r.port)}},
		})
	}
	_ = r.cc.UpdateState(resolver.State{Endpoints: endpoints})
}

// ResolveNow does nothing: the ticker looks up often enough, and gRPC calls this
// on every failed connection.
func (r *headlessResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *headlessResolver) Close() {
	r.cancel()
	<-r.done
}
