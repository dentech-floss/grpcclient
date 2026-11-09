# grpcclient

Dials another service over gRPC, on Cloud Run or inside a Kubernetes cluster, so the same image works in both places.

| Mode | Target | Transport | Auth |
| --- | --- | --- | --- |
| `cloudrun` (default) | `<service>-<hash>-lz.a.run.app:443` | TLS | Google ID token |
| `cluster` | `<service>.<namespace>.svc.cluster.local:80` | plaintext | none |

Both modes add OpenTelemetry and retry `Aborted`, `Unavailable`, `Canceled` and `Internal` up to 3 times.

## Install

```
go get github.com/dentech-floss/grpcclient@latest
```

## Usage

```go
conn, err := grpcclient.NewConn(ctx, "example-service", &grpcclient.Config{
    CloudRunProjectHash: *config.CloudRunProjectHash,
})
if err != nil {
    panic(err)
}
client := examplev1grpc.NewExampleServiceClient(conn)
```

## Settings

| Field | Environment | |
| --- | --- | --- |
| `Mode` | `SERVICE_ROUTING` | `cloudrun` or `cluster`. Unset means `cloudrun`, any other value is an error |
| `CloudRunProjectHash` | | The hash in the `run.app` host. Required for `cloudrun` |
| `Namespace` | `SERVICE_NAMESPACE` | The target's namespace. Required for `cluster` |
| `ClusterPort` | | The target's Service port. Default 80 |

A field that is set wins over the environment. Set the environment in the deployment, so the code stays the same in both places.

## Good to know

- **In `cluster` mode the traffic is unencrypted and unauthenticated.** Restrict who may call a service with a NetworkPolicy.
- **In `cluster` mode a client sticks to one pod.** A ClusterIP Service gives gRPC one address, and gRPC keeps one long-lived connection to it, so calls aren't balanced across replicas. Use a headless Service and `round_robin` if a service needs that.

## Development

```
make test
```
