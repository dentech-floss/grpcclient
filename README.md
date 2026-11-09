# grpcclient

Dials another service over gRPC, on Cloud Run or inside a Kubernetes cluster, so the same image works in both places.

| Mode | Target | Transport | Auth |
| --- | --- | --- | --- |
| `cloudrun` (default) | `<service>-<hash>-lz.a.run.app:443` | TLS | Google ID token |
| `cluster` | every pod behind `<service>-headless.<namespace>.svc.cluster.local:8080` | plaintext | none |

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
| `ClusterPort` | | The port the target's pods listen on. Default 8080 |

A field that is set wins over the environment. Set the environment in the deployment, so the code stays the same in both places.

## Good to know

- **In `cluster` mode the traffic is unencrypted and unauthenticated.** Restrict who may call a service with a NetworkPolicy.
- **In `cluster` mode the target needs a headless Service named `<service>-headless`.** That's a Service with `clusterIP: None` that selects the same pods as the normal one. It runs nothing: it's a DNS name that returns every pod's IP. Have your deployment template create one for every service, and deploy the target with it before a client on this version calls it.
- **In `cluster` mode calls go to all replicas, in turn.** The client looks up the pods every 5 seconds through DNS. How soon it sees a change also depends on your DNS cache, and a new replica only gets calls once the client has connected to it. If a lookup fails, the client keeps the addresses it has.
- **Let a stopping pod keep serving until clients know its replacement.** A fast rollout can remove the old pods before clients have learned the new ones, and every call in that gap fails. Use a rolling update, so new pods are ready before old ones stop, and give pods a `preStop` sleep longer than the lookup interval plus your DNS cache time. Set `terminationGracePeriodSeconds` to cover both the sleep and the app's own shutdown. This closes the gap in normal rollouts, but not while lookups are failing.

## Development

```
make test
```
