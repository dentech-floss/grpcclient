package grpcclient

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
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

// Cluster names don't resolve here: hand them to the dialer as they are.
func TestMain(m *testing.M) {
	resolver.SetDefaultScheme("passthrough")
	os.Exit(m.Run())
}

// resolveLocal sends every address to 127.0.0.1, standing in for cluster DNS.
func resolveLocal(port int, seen *atomic.Value) grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		seen.Store(addr)
		var d net.Dialer
		return d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	})
}

func TestCluster_CallsTheServiceInPlaintext(t *testing.T) {
	port := startServer(t, nil)
	var seen atomic.Value

	conn, err := NewConn(
		context.Background(), "clinic-service",
		&Config{Mode: ModeCluster, Namespace: "ns"},
		resolveLocal(port, &seen),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if got, want := seen.Load(), "clinic-service.ns.svc.cluster.local:80"; got != want {
		t.Errorf("dialed %q, want %q", got, want)
	}
}

func TestCluster_RetriesUnavailable(t *testing.T) {
	var calls atomic.Int32
	port := startServer(t, func() error {
		if calls.Add(1) < 3 {
			return status.Error(codes.Unavailable, "not yet")
		}
		return nil
	})
	var seen atomic.Value

	conn, err := NewConn(
		context.Background(), "clinic-service",
		&Config{Mode: ModeCluster, Namespace: "ns"},
		resolveLocal(port, &seen),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}
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

	if got, want := conn.Target(), "svc.from-env.svc.cluster.local:80"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

func TestCluster_ConfigBeatsEnvironmentAndPortIsUsed(t *testing.T) {
	t.Setenv(EnvMode, ModeCloudRun)
	t.Setenv(EnvNamespace, "from-env")

	conn, err := NewConn(context.Background(), "svc", &Config{Mode: ModeCluster, Namespace: "ns", ClusterPort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got, want := conn.Target(), "svc.ns.svc.cluster.local:8080"; got != want {
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
