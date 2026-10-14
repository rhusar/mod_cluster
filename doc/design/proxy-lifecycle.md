# Proxy lifecycle

* Issue: [MODCLUSTER-879](https://redhat.atlassian.net/browse/MODCLUSTER-879) – ModClusterService can be 'established' before CONFIG message is sent to the proxy
* Related: [WFLY-22198](https://redhat.atlassian.net/browse/WFLY-22198) – Race condition in mod_cluster startup: STOP-APP can be sent before CONFIG

## Problem

A proxy (mod_proxy_cluster) only knows about a node after it has processed a `CONFIG` command for it.
Any node-specific command (`ENABLE-APP`, `DISABLE-APP`, `STOP-APP`, `REMOVE-APP`, `STATUS`) sent before that is rejected:

```
ERROR [org.jboss.modcluster] MODCLUSTER000042: Error MEM sending STOP-APP command to /127.0.0.1:6666, configuration will be reset: MEM: Can't read node with "node1" JVMRoute
```

The proxy is then put in error and is reset on the next periodic status, which may again race with container events.

Before this change, the state that decided whether commands could be sent was tracked in two places, and neither tracked whether `CONFIG` had been processed:

1. `ModClusterService.established` – a global latch set by `connectionEstablished(..)` after the first successful `INFO` to *any* proxy.
   It never turned `false` again: not when all proxies went into error, not when the last proxy was removed, and it did not account for multiple proxies.
2. `DefaultMCMPHandler.Proxy.state` (`OK`/`ERROR`/`DOWN`) – a proxy became `OK` on *any* successful response, including the `INFO` that starts the reset.
   Between that `INFO` and the subsequent `CONFIG` (both sent from the periodic `status()`), any concurrent container event was sent to the now `OK` proxy and failed with `MEM`.
   The per-proxy `established` flag was set on a successful `CONFIG`, but was never cleared and never consulted.

## Design

Each proxy has a lifecycle (`DefaultMCMPHandler.Proxy.Lifecycle`), which is the single source of truth for which requests may be sent to it:

| Lifecycle     | Meaning                                                     | Accepted requests      | `MCMPServerState.State` | `isEstablished()` |
|---------------|-------------------------------------------------------------|------------------------|-------------------------|-------------------|
| `ERROR`       | Not (yet) reachable; initial state of every added proxy      | `INFO`                 | `ERROR`                 | `false`           |
| `CONNECTED`   | `INFO` succeeded, but the node has not been configured      | `INFO`, `DUMP`, `PING` | `ERROR`                 | `false`           |
| `ESTABLISHED` | Handshake completed, i.e. `CONFIG` and reset requests succeeded | all                 | `OK`                    | `true`            |
| `DOWN`        | Unrecoverable (`SYNTAX`) error                              | `INFO`                 | `DOWN`                  | `false`           |

The public `MCMPServerState.State` enum is unchanged; `CONNECTED` is reported as `ERROR`, since a proxy that does not know the node is not healthy from the node's point of view.

### Transitions

```
                 add / markProxiesInError() / reset()
                                  │
                                  ▼
 ┌─────────┐ INFO ok  ┌───────────┐ reset requests ok (incl. CONFIG) ┌─────────────┐
 │  ERROR  │─────────▶│ CONNECTED │─────────────────────────────────▶│ ESTABLISHED │
 └─────────┘          └───────────┘◀─────────────────────────────────└─────────────┘
   ▲  ▲  ▲                  │               REMOVE-APP * ok                 │
   │  │  └──────────────────┘                                               │
   │  └───────────────── any failure (non-200, I/O error) ──────────────────┘
   │
   │ reset()   ┌──────┐
   └───────────│ DOWN │◀── SYNTAX error response (from any state)
               └──────┘
```

* **Handshake** (`DefaultMCMPHandler.handshake(..)`), triggered by the periodic `status()` for proxies in `ERROR` or `CONNECTED`, and by `establish()` for proxies in `CONNECTED`:
  1. `INFO`; on success, the proxy becomes `CONNECTED`, and the `MCMPConnectionListener` is notified once (connector address and jvmRoute detection).
  2. The reset requests are computed from the `INFO` response and the current container state (`ResetRequestSource`), i.e. `[REMOVE-APP *] CONFIG {ENABLE-APP|DISABLE-APP|STOP-APP|REMOVE-APP}*`.
  3. Once all of them succeeded *and* they included a `CONFIG`, the proxy becomes `ESTABLISHED`. If there is nothing to configure (e.g. no engine with a connector), the proxy stays `CONNECTED` and the handshake is retried on the next status.
* **Any failure** – non-200 response, I/O error, or the connection closed without a response – moves the proxy to `ERROR` (or `DOWN` for a `SYNTAX` error), which revokes `ESTABLISHED`.
  A successful response never promotes a proxy; only a completed handshake does.
* **`REMOVE-APP *`** removes the node from the proxy, so a successful one moves an `ESTABLISHED` proxy back to `CONNECTED`.
  This models `ModClusterService.stop(Server)`, after which `start(Server)` re-establishes the proxies via `MCMPHandler.establish()`.
  This previously relied on `start(Server)` sending `CONFIG` to all `OK` proxies.
* **`markProxiesInError()`** (`refresh` operation) moves `CONNECTED`/`ESTABLISHED` to `ERROR`; **`reset()`** moves `DOWN` to `ERROR`.
* The deprecated `addProxy(.., boolean established)` overloads ignore the flag, since a proxy cannot be considered established without a handshake.

### Concurrency

Context commands come from container threads, while the handshake runs from the periodic status thread (or from `start(Server)`).
The ordering guarantee relies on the proxy monitor, which already serialized socket access:

* The handshake holds the proxy monitor from the `INFO` through the last reset request and the transition to `ESTABLISHED`.
* The lifecycle check for a regular request (`sendRequest(MCMPRequest, Proxy)`) is performed while holding the same monitor, immediately followed by the send.
* Lifecycle transitions requested from outside (`markProxiesInError()`, `reset()`) also hold the monitor, so they are not overwritten by an in-flight handshake.

Consequently, for a container event that changes a context state and then sends a command, either:

* the command is checked *before* the handshake acquired the monitor – it is rejected, but the reset requests are computed *afterwards* from the already updated container state, so the change is included in them; or
* the command is checked *after* the handshake released the monitor – the proxy is `ESTABLISHED` and the command is sent (possibly duplicating a reset request, which is idempotent).

No command is lost, and no command reaches a proxy before its `CONFIG`.

For the same reason, `ModClusterService` must not skip a context command just because no proxy is established *yet*.
Its context event methods (`add`, `start`, `stop`, `remove`) are therefore gated on the `connected` flag – set once by `connectionEstablished(..)` before any handshake computes its reset requests – which only guarantees that the jvmRoute required to build the requests has been initialized.
Everything else is left to the per-proxy lifecycle.

Note that the handshake reads container state (`Context.isStarted()` etc.) while holding the proxy monitor.
This is the same container state access as before (it previously ran under the proxies read lock), but container implementations must not block on a lock held by a thread that is waiting to send a command.

### `ModClusterService`

* `isEstablished()` delegates to `MCMPHandler.isEstablished()` – `true` if *at least one* proxy is currently `ESTABLISHED`.
  It turns `false` when all proxies fail or the last proxy is removed.
* Server-wide operations (`stop(Server)`, `status(Engine)`, `enable()`, `disable()`, `enableContext(..)`, `disableContext(..)`, `stop(..)`, `stopContext(..)`) are gated on `isEstablished()`.
* Context event methods are gated on `connected` (see above). Session draining in `stop(Context)` is skipped when no proxy is established, since no proxy routes requests to the node.
* Diagnostic operations (`getProxyInfo()`, `getProxyConfiguration()`, `ping(..)`) are gated on `connected`; `INFO`, `DUMP` and `PING` are accepted by `CONNECTED` proxies.
* `reset()` and `refresh()` are no longer gated – resetting `DOWN` proxies is precisely useful when nothing is established.
* `start(Server)` calls `MCMPHandler.establish()` instead of sending `CONFIG` directly; `config(Engine)` is deprecated.

### API changes

* `MCMPHandler.isEstablished()` – new default method (any proxy `isEstablished()`).
* `MCMPHandler.establish()` – new default method (falls back to `status()`); completes the handshake with reachable, not yet established proxies.
* `MCMPHandler.addProxy(ProxyConfiguration, boolean)` – deprecated; the `established` argument is ignored.
* `MCMPServer.isEstablished()` – now reflects the actual state: the proxy has been configured and accepts all requests.
* `MCMPServerState.getState()` – `OK` now strictly means established.

## Startup sequence

`MCMPHandler.init(..)` only queues the configured proxies; they are contacted on the first periodic `status()`.
The resulting sequence per proxy is always:

```
INFO → [REMOVE-APP *] → CONFIG → {STOP-APP | ENABLE-APP | DISABLE-APP}* → (proxy ESTABLISHED) → container events…
```

## Testing

`DefaultMCMPHandlerTestCase` runs the handler against a minimal in-process MCMP proxy which, like mod_proxy_cluster, rejects node commands with a `MEM` error until it has received `CONFIG`.
It covers the handshake on the periodic status, deferral of a command issued while the handshake is in progress, re-establishing after `REMOVE-APP *` and after the proxy lost its configuration, `markProxiesInError()`, removal of the last proxy, multiple proxies with one unreachable, and the deprecated `addProxy(.., true)`.
