// Package grpcclient dials another service over gRPC, on Cloud Run or inside a
// Kubernetes cluster, so the same image works in both places.
package grpcclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
)

const (
	// ModeCloudRun dials https://<service>-<hash>-lz.a.run.app with a Google ID token.
	ModeCloudRun = "cloudrun"
	// ModeCluster dials <service>.<namespace>.svc.cluster.local in plaintext, with no token.
	ModeCluster = "cluster"

	// EnvMode is read when Config.Mode is empty. Unset means ModeCloudRun.
	EnvMode = "SERVICE_ROUTING"
	// EnvNamespace is read when Config.Namespace is empty.
	EnvNamespace = "SERVICE_NAMESPACE"

	defaultClusterPort = 80
)

// Config says how to reach other services. Mode and Namespace fall back to the
// environment, so a deployment picks the mode and the code stays the same.
type Config struct {
	// Mode is ModeCloudRun or ModeCluster. Empty: EnvMode, then ModeCloudRun.
	Mode string
	// CloudRunProjectHash is the hash in the run.app host. Required for ModeCloudRun.
	CloudRunProjectHash string
	// Namespace is the target's namespace. Empty: EnvNamespace. Required for ModeCluster.
	Namespace string
	// ClusterPort is the target's Service port. Zero means 80.
	ClusterPort int
}

// newTokenSource is replaced in tests, which have no Google credentials.
var newTokenSource = func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
	return idtoken.NewTokenSource(ctx, audience)
}

// NewConn returns a client connection to serviceName, with OpenTelemetry and
// retries on Aborted, Unavailable, Canceled and Internal. Extra options are
// applied last.
func NewConn(
	ctx context.Context,
	serviceName string,
	config *Config,
	extra ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("grpcclient: service name is empty")
	}
	if config == nil {
		config = &Config{}
	}

	mode := config.Mode
	if mode == "" {
		mode = os.Getenv(EnvMode)
	}

	var (
		target string
		opts   []grpc.DialOption
		err    error
	)
	switch mode {
	case "", ModeCloudRun:
		target, opts, err = cloudRun(ctx, serviceName, config)
	case ModeCluster:
		target, opts, err = cluster(serviceName, config)
	default:
		return nil, fmt.Errorf("grpcclient: unknown mode %q, want %q or %q", mode, ModeCloudRun, ModeCluster)
	}
	if err != nil {
		return nil, err
	}

	opts = append(opts, commonOptions()...)
	opts = append(opts, extra...)

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: dial %s: %w", target, err)
	}
	return conn, nil
}

func cloudRun(ctx context.Context, serviceName string, config *Config) (string, []grpc.DialOption, error) {
	if config.CloudRunProjectHash == "" {
		return "", nil, fmt.Errorf("grpcclient: mode %q needs CloudRunProjectHash", ModeCloudRun)
	}
	host := fmt.Sprintf("%s-%s-lz.a.run.app", serviceName, config.CloudRunProjectHash)

	tokenSource, err := newTokenSource(ctx, "https://"+host)
	if err != nil {
		return "", nil, fmt.Errorf("grpcclient: token source for %s: %w", host, err)
	}
	systemRoots, err := x509.SystemCertPool()
	if err != nil {
		return "", nil, fmt.Errorf("grpcclient: system cert pool: %w", err)
	}

	target := host + ":443"
	return target, []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: systemRoots})),
		grpc.WithAuthority(target),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}),
	}, nil
}

func cluster(serviceName string, config *Config) (string, []grpc.DialOption, error) {
	namespace := config.Namespace
	if namespace == "" {
		namespace = os.Getenv(EnvNamespace)
	}
	if namespace == "" {
		return "", nil, fmt.Errorf("grpcclient: mode %q needs Namespace or %s", ModeCluster, EnvNamespace)
	}
	port := config.ClusterPort
	if port == 0 {
		port = defaultClusterPort
	}

	target := fmt.Sprintf("%s.%s.svc.cluster.local:%d", serviceName, namespace, port)
	return target, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, nil
}

func commonOptions() []grpc.DialOption {
	retry := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100 * time.Millisecond)),
		grpc_retry.WithMax(3),
		grpc_retry.WithCodes(
			codes.Aborted,
			codes.Unavailable,
			codes.Canceled,
			codes.Internal,
		),
	}
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retry...)),
		grpc.WithStreamInterceptor(grpc_retry.StreamClientInterceptor(retry...)),
	}
}
