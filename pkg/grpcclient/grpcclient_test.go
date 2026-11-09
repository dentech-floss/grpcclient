package grpcclient

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func fakeTokenSource(t *testing.T) *string {
	t.Helper()
	original := newTokenSource
	t.Cleanup(func() { newTokenSource = original })

	audience := new(string)
	newTokenSource = func(_ context.Context, a string) (oauth2.TokenSource, error) {
		*audience = a
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}), nil
	}
	return audience
}

// startServer listens on 127.0.0.1 and returns the port. fail, when non-nil, is
// called before every health check and may return an error.
func startServer(t *testing.T, fail func() error) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		if fail != nil {
			if err := fail(); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().(*net.TCPAddr).Port
}

// fakeCluster stands in for cluster DNS and the pod network: pods get made-up IPs
// that the dialer maps to local servers.
type fakeCluster struct {
	mu      sync.Mutex
	pods    map[string]int // pod IP -> local port
	err     error
	empty   bool
	lookups []string
	dialed  []string
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	cluster := &fakeCluster{pods: map[string]int{}}

	originalLookup, originalInterval, originalRetry := lookupHost, lookupInterval, firstLookupRetry
	t.Cleanup(func() { lookupHost, lookupInterval, firstLookupRetry = originalLookup, originalInterval, originalRetry })
	lookupHost, lookupInterval, firstLookupRetry = cluster.lookup, 5*time.Millisecond, 5*time.Millisecond
	return cluster
}

func (c *fakeCluster) lookup(_ context.Context, host string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups = append(c.lookups, host)
	if c.err != nil {
		return nil, c.err
	}
	if c.empty {
		return nil, nil
	}
	return slices.Collect(maps.Keys(c.pods)), nil
}

func (c *fakeCluster) dialer() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.dialed = append(c.dialed, addr)
		port, ok := c.pods[host]
		c.mu.Unlock()
		if !ok {
			return nil, errors.New("no such pod")
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	})
}

// addPod starts a server that counts its calls and makes it resolvable.
func (c *fakeCluster) addPod(t *testing.T, ip string) *atomic.Int32 {
	t.Helper()
	calls := new(atomic.Int32)
	port := startServer(t, func() error {
		calls.Add(1)
		return nil
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pods[ip] = port
	return calls
}

func (c *fakeCluster) removePod(ip string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pods, ip)
}

func (c *fakeCluster) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *fakeCluster) setEmpty(empty bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.empty = empty
}

func dialCluster(t *testing.T, cluster *fakeCluster) healthpb.HealthClient {
	t.Helper()
	conn, err := NewConn(
		context.Background(), "clinic-service",
		&Config{Mode: ModeCluster, Namespace: "ns"},
		cluster.dialer(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func check(ctx context.Context, client healthpb.HealthClient) error {
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func mustCheck(t *testing.T, client healthpb.HealthClient, times int) {
	t.Helper()
	for range times {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := check(ctx, client)
		cancel()
		if err != nil {
			t.Fatalf("health check: %v", err)
		}
	}
}

// eventually calls the service until done is true.
func eventually(t *testing.T, client healthpb.HealthClient, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s", what)
		}
		mustCheck(t, client, 1)
		time.Sleep(time.Millisecond)
	}
}

func TestCluster_CallsThePodsOfTheHeadlessServiceInPlaintext(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.addPod(t, "10.0.0.1")

	mustCheck(t, dialCluster(t, cluster), 1)

	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if got, want := cluster.lookups[0], "clinic-service-headless.ns.svc.cluster.local."; got != want {
		t.Errorf("looked up %q, want %q", got, want)
	}
	if got, want := cluster.dialed[0], "10.0.0.1:8080"; got != want {
		t.Errorf("dialed %q, want %q", got, want)
	}
}

func TestCluster_BalancesAcrossPods(t *testing.T) {
	cluster := newFakeCluster(t)
	first := cluster.addPod(t, "10.0.0.1")
	second := cluster.addPod(t, "10.0.0.2")
	client := dialCluster(t, cluster)

	eventually(t, client, "both pods called", func() bool { return first.Load() > 0 && second.Load() > 0 })

	firstBefore, secondBefore := first.Load(), second.Load()
	mustCheck(t, client, 100)
	if got := first.Load() - firstBefore; got != 50 {
		t.Errorf("first pod got %d of 100 calls, want 50", got)
	}
	if got := second.Load() - secondBefore; got != 50 {
		t.Errorf("second pod got %d of 100 calls, want 50", got)
	}
}

func TestCluster_NewPodGetsTrafficAndRemovedPodDoesNot(t *testing.T) {
	cluster := newFakeCluster(t)
	first := cluster.addPod(t, "10.0.0.1")
	client := dialCluster(t, cluster)
	mustCheck(t, client, 1)

	second := cluster.addPod(t, "10.0.0.2")
	eventually(t, client, "new pod called", func() bool { return second.Load() > 0 })

	// While the removed pod is still in the list, 20 calls in turn always reach it.
	cluster.removePod("10.0.0.1")
	eventually(t, client, "removed pod no longer called", func() bool {
		before := first.Load()
		mustCheck(t, client, 20)
		return first.Load() == before
	})
}

func TestCluster_FailedOrEmptyLookupKeepsTheAddresses(t *testing.T) {
	tests := []struct {
		name string
		fail func(*fakeCluster)
		heal func(*fakeCluster)
	}{
		{
			"error",
			func(c *fakeCluster) { c.setErr(errors.New("dns down")) },
			func(c *fakeCluster) { c.setErr(nil) },
		},
		{
			"empty answer",
			func(c *fakeCluster) { c.setEmpty(true) },
			func(c *fakeCluster) { c.setEmpty(false) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newFakeCluster(t)
			first := cluster.addPod(t, "10.0.0.1")
			client := dialCluster(t, cluster)
			mustCheck(t, client, 1)

			tt.fail(cluster)
			time.Sleep(50 * time.Millisecond)
			before := first.Load()
			mustCheck(t, client, 5)
			if got := first.Load() - before; got != 5 {
				t.Errorf("pod got %d calls during the failure, want 5", got)
			}

			tt.heal(cluster)
			second := cluster.addPod(t, "10.0.0.2")
			eventually(t, client, "lookups resumed", func() bool { return second.Load() > 0 })
		})
	}
}

func TestCluster_FirstLookupFailingFailsCallsThenRecovers(t *testing.T) {
	cluster := newFakeCluster(t)
	lookupInterval = time.Hour // only the quicker first-lookup retry can recover in time
	cluster.setErr(errors.New("dns down"))
	client := dialCluster(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := check(ctx, client); status.Code(err) != codes.Unavailable {
		t.Errorf("err = %v, want Unavailable", err)
	}

	cluster.setErr(nil)
	calls := cluster.addPod(t, "10.0.0.1")
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the client to recover")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = check(ctx, client)
		cancel()
	}
}

func TestCluster_RetriesUnavailable(t *testing.T) {
	cluster := newFakeCluster(t)
	var calls atomic.Int32
	port := startServer(t, func() error {
		if calls.Add(1) < 3 {
			return status.Error(codes.Unavailable, "not yet")
		}
		return nil
	})
	cluster.pods["10.0.0.1"] = port

	mustCheck(t, dialCluster(t, cluster), 1)

	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d calls, want 3", got)
	}
}

func TestCluster_ModeAndNamespaceFromEnvironment(t *testing.T) {
	t.Setenv(EnvMode, ModeCluster)
	t.Setenv(EnvNamespace, "from-env")

	conn, err := NewConn(context.Background(), "svc", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got, want := conn.Target(), "headless:///svc-headless.from-env.svc.cluster.local:8080"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

func TestCluster_ConfigBeatsEnvironmentAndPortIsUsed(t *testing.T) {
	t.Setenv(EnvMode, ModeCloudRun)
	t.Setenv(EnvNamespace, "from-env")

	conn, err := NewConn(context.Background(), "svc", &Config{Mode: ModeCluster, Namespace: "ns", ClusterPort: 9090})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got, want := conn.Target(), "headless:///svc-headless.ns.svc.cluster.local:9090"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

func TestCloudRun_IsTheDefault(t *testing.T) {
	t.Setenv(EnvMode, "")
	audience := fakeTokenSource(t)

	conn, err := NewConn(context.Background(), "svc", &Config{CloudRunProjectHash: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got, want := conn.Target(), "svc-abc-lz.a.run.app:443"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
	if got, want := *audience, "https://svc-abc-lz.a.run.app"; got != want {
		t.Errorf("audience = %q, want %q", got, want)
	}
}

func TestCloudRun_TokenSourceErrorIsReturned(t *testing.T) {
	original := newTokenSource
	t.Cleanup(func() { newTokenSource = original })
	boom := errors.New("no credentials")
	newTokenSource = func(context.Context, string) (oauth2.TokenSource, error) { return nil, boom }

	_, err := NewConn(context.Background(), "svc", &Config{Mode: ModeCloudRun, CloudRunProjectHash: "abc"})
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want it to wrap %v", err, boom)
	}
}

func TestErrors(t *testing.T) {
	t.Setenv(EnvMode, "")
	t.Setenv(EnvNamespace, "")

	tests := []struct {
		name    string
		service string
		config  *Config
		want    string
	}{
		{"empty service", "", &Config{Mode: ModeCluster, Namespace: "ns"}, "service name is empty"},
		{"unknown mode", "svc", &Config{Mode: "mesh"}, `unknown mode "mesh"`},
		{"cluster without namespace", "svc", &Config{Mode: ModeCluster}, "needs Namespace"},
		{"cloud run without hash", "svc", &Config{Mode: ModeCloudRun}, "needs CloudRunProjectHash"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConn(context.Background(), tt.service, tt.config)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}
