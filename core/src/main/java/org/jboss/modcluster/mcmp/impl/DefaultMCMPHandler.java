/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster.mcmp.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.SocketFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jboss.logging.Logger;
import org.jboss.modcluster.ModClusterLogger;
import org.jboss.modcluster.config.MCMPHandlerConfiguration;
import org.jboss.modcluster.config.ProxyConfiguration;
import org.jboss.modcluster.mcmp.MCMPConnectionListener;
import org.jboss.modcluster.mcmp.MCMPHandler;
import org.jboss.modcluster.mcmp.MCMPRequest;
import org.jboss.modcluster.mcmp.MCMPRequestFactory;
import org.jboss.modcluster.mcmp.MCMPRequestType;
import org.jboss.modcluster.mcmp.MCMPResponseParser;
import org.jboss.modcluster.mcmp.MCMPServer;
import org.jboss.modcluster.mcmp.MCMPServerState;
import org.jboss.modcluster.mcmp.MCMPServerState.State;
import org.jboss.modcluster.mcmp.ResetRequestSource;

/**
 * Default implementation of {@link MCMPHandler}.
 *
 * @author Jean-Frederic Clere
 * @author Brian Stansberry
 * @author Paul Ferraro
 */
@ThreadSafe
public class DefaultMCMPHandler implements MCMPHandler {
    private static final String NEW_LINE = "\r\n";

    static final Logger log = Logger.getLogger(DefaultMCMPHandler.class);

    // -------------------------------------------------------------- Constants

    // ----------------------------------------------------------------- Fields

    private final MCMPHandlerConfiguration config;
    /** Source for reset requests when we need to reset a proxy. */
    private final ResetRequestSource resetRequestSource;
    private final MCMPRequestFactory requestFactory;
    private final MCMPResponseParser responseParser;

    private final ReadWriteLock proxiesLock = new ReentrantReadWriteLock();
    private final Lock addRemoveProxiesLock = new ReentrantLock();

    /** Proxies. */
    @GuardedBy("proxiesLock")
    private final List<Proxy> proxies = new ArrayList<Proxy>();

    /** Add proxy list. */
    @GuardedBy("addRemoveProxiesLock")
    private final List<Proxy> addProxies = new ArrayList<Proxy>();

    /** Remove proxy list. */
    @GuardedBy("addRemoveProxiesLock")
    private final List<Proxy> removeProxies = new ArrayList<Proxy>();

    /** Whether the connection listener has been notified of the first successful connection to any proxy. */
    private final AtomicBoolean connectionListenerNotified = new AtomicBoolean(false);
    private volatile MCMPConnectionListener connectionListener;
    private volatile boolean init = false;

    // ----------------------------------------------------------- Constructors

    public DefaultMCMPHandler(MCMPHandlerConfiguration config, ResetRequestSource source, MCMPRequestFactory requestFactory,
            MCMPResponseParser responseParser) {
        this.resetRequestSource = source;
        this.config = config;
        this.requestFactory = requestFactory;
        this.responseParser = responseParser;
    }

    // ------------------------------------------------------------ MCMPHandler

    @Override
    public void init(Collection<ProxyConfiguration> proxies, MCMPConnectionListener connectionListener) {
        this.connectionListener = connectionListener;

        if (proxies != null) {
            Lock lock = this.proxiesLock.writeLock();
            lock.lock();

            try {
                for (final ProxyConfiguration proxy : proxies) {
                    this.add(proxy.getRemoteAddress(), proxy.getLocalAddress());
                }

                this.status(false);
            } finally {
                lock.unlock();
            }
        }

        this.init = true;
    }

    @Override
    public void shutdown() {
        this.init = false;

        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                proxy.closeConnection();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Deprecated
    public void addProxy(InetSocketAddress socketAddress) {
        this.add(socketAddress);
    }

    @Override
    public void addProxy(ProxyConfiguration proxyConfiguration) {
        this.add(proxyConfiguration.getRemoteAddress(), proxyConfiguration.getLocalAddress());
    }

    private Proxy add(InetSocketAddress socketAddress) {
        return this.add(socketAddress, null);
    }

    private Proxy add(InetSocketAddress socketAddress, InetSocketAddress localAddress) {
        Proxy proxy = new Proxy(socketAddress, localAddress, this.config);

        this.addRemoveProxiesLock.lock();

        try {
            Lock lock = this.proxiesLock.readLock();
            lock.lock();

            try {
                for (Proxy candidate : this.proxies) {
                    if (candidate.equals(proxy))
                        return candidate;
                }
            } finally {
                lock.unlock();
            }

            for (Proxy candidate : this.addProxies) {
                if (candidate.equals(proxy))
                    return candidate;
            }
            for (Proxy candidate : this.removeProxies) {
                if (candidate.equals(proxy))
                    return candidate;
            }

            this.addProxies.add(proxy);
        } finally {
            this.addRemoveProxiesLock.unlock();
        }

        return proxy;
    }

    @Override
    @Deprecated
    public void addProxy(InetSocketAddress socketAddress, boolean established) {
        // A proxy can only become established by completing the handshake, see MODCLUSTER-879
        this.add(socketAddress);
    }

    @Override
    @Deprecated
    public void addProxy(ProxyConfiguration proxyConfiguration, boolean established) {
        // A proxy can only become established by completing the handshake, see MODCLUSTER-879
        this.add(proxyConfiguration.getRemoteAddress(), proxyConfiguration.getLocalAddress());
    }

    @Override
    public void removeProxy(InetSocketAddress socketAddress) {
        Proxy proxy = new Proxy(socketAddress, this.config);

        this.addRemoveProxiesLock.lock();

        try {
            this.removeProxies.add(proxy);
        } finally {
            this.addRemoveProxiesLock.unlock();
        }
    }

    @Override
    public Set<MCMPServerState> getProxyStates() {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            if (this.proxies.isEmpty())
                return Collections.emptySet();

            Set<MCMPServerState> result = new LinkedHashSet<MCMPServerState>(this.proxies.size());

            for (Proxy proxy : this.proxies) {
                result.add(proxy);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isProxyHealthOK() {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                if (proxy.getState() != MCMPServerState.State.OK) {
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void markProxiesInError() {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                // Synchronize with any in-flight request or handshake, so that this transition is not overwritten
                synchronized (proxy) {
                    if (proxy.getLifecycle() == Proxy.Lifecycle.CONNECTED || proxy.getLifecycle() == Proxy.Lifecycle.ESTABLISHED) {
                        proxy.setLifecycle(Proxy.Lifecycle.ERROR);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                synchronized (proxy) {
                    if (proxy.getLifecycle() == Proxy.Lifecycle.DOWN) {
                        proxy.setLifecycle(Proxy.Lifecycle.ERROR);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEstablished() {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                if (proxy.isEstablished()) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized void status() {
        if (this.init) {
            this.processPendingDiscoveryEvents();

            this.status(true);
        }
    }

    @Override
    public synchronized void establish() {
        if (this.init) {
            Lock lock = this.proxiesLock.readLock();
            lock.lock();

            try {
                for (Proxy proxy : this.proxies) {
                    // Only complete handshakes with proxies that are known to be reachable
                    if (proxy.getLifecycle() == Proxy.Lifecycle.CONNECTED) {
                        this.handshake(proxy, true);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Send a periodic status request.
     *
     * @param sendResetRequests if enabled, the handler will attempt to (re)configure the front end server and thus establish the
     *        proxy; otherwise only connectivity is verified
     */
    private void status(boolean sendResetRequests) {
        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                Proxy.Lifecycle lifecycle = proxy.getLifecycle();

                // Attempt to (re)establish any proxies in error or which have not yet been configured
                if (lifecycle == Proxy.Lifecycle.ERROR || (sendResetRequests && lifecycle == Proxy.Lifecycle.CONNECTED)) {
                    this.handshake(proxy, sendResetRequests);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Performs the handshake with the given proxy: INFO, followed by the reset requests (including CONFIG). The proxy becomes
     * {@link Proxy.Lifecycle#ESTABLISHED} only after all reset requests succeeded. The proxy monitor is held for the whole
     * handshake, so that any concurrent request is either rejected before the reset requests are computed (and thus is
     * reflected by them) or is sent only after the proxy is established.
     *
     * @param proxy the proxy to handshake with
     * @param sendResetRequests whether to configure the proxy, or only verify connectivity
     */
    private void handshake(Proxy proxy, boolean sendResetRequests) {
        synchronized (proxy) {
            Proxy.Lifecycle lifecycle = proxy.getLifecycle();

            if (lifecycle == Proxy.Lifecycle.ERROR) {
                proxy.closeConnection();
            } else if (lifecycle != Proxy.Lifecycle.CONNECTED) {
                // State changed in the meantime
                return;
            }

            Response info = this.send(this.requestFactory.createInfoRequest(), proxy);

            if (!info.isSuccess()) {
                return;
            }

            proxy.setLifecycle(Proxy.Lifecycle.CONNECTED);

            // Only notify connection listener once
            if (this.connectionListenerNotified.compareAndSet(false, true)) {
                this.connectionListener.connectionEstablished(proxy.getLocalAddress());
            }

            if (!sendResetRequests) {
                return;
            }

            Map<String, Set<ResetRequestSource.VirtualHost>> parsedResponse = this.responseParser.parseInfoResponse(info.getBody());

            List<MCMPRequest> requests = this.resetRequestSource.getResetRequests(parsedResponse);

            log.trace(requests);

            boolean configured = false;

            for (MCMPRequest request : requests) {
                if (!this.send(request, proxy).isSuccess()) {
                    // Proxy was already transitioned to ERROR or DOWN; the next periodic status will retry
                    return;
                }
                configured |= request.getRequestType() == MCMPRequestType.CONFIG;
            }

            // Without a CONFIG (e.g. no engine is available yet) the node does not exist on the proxy
            if (configured) {
                proxy.setLifecycle(Proxy.Lifecycle.ESTABLISHED);
            }
        }
    }

    @Override
    public Map<MCMPServerState, String> sendRequest(MCMPRequest request) {
        Map<MCMPServerState, String> map = new HashMap<MCMPServerState, String>();

        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                map.put(proxy, this.sendRequest(request, proxy));
            }
        } finally {
            lock.unlock();
        }

        return map;
    }

    @Override
    public Map<MCMPServerState, List<String>> sendRequests(List<MCMPRequest> requests) {
        Map<MCMPServerState, List<String>> map = new HashMap<MCMPServerState, List<String>>();

        Lock lock = this.proxiesLock.readLock();
        lock.lock();

        try {
            for (Proxy proxy : this.proxies) {
                List<String> list = new ArrayList<String>(requests.size());

                for (MCMPRequest request : requests) {
                    list.add(this.sendRequest(request, proxy));
                }

                map.put(proxy, list);
            }
        } finally {
            lock.unlock();
        }

        return map;
    }

    // ---------------------------------------------------------------- Private

    private void processPendingDiscoveryEvents() {
        this.addRemoveProxiesLock.lock();

        try {
            // Check to add or remove proxies, and rebuild a new list if needed
            if (!this.addProxies.isEmpty() || !this.removeProxies.isEmpty()) {
                Lock lock = this.proxiesLock.writeLock();
                lock.lock();

                try {
                    this.proxies.addAll(this.addProxies);
                    this.proxies.removeAll(this.removeProxies);

                    this.addProxies.clear();
                    this.removeProxies.clear();

                    // Reset all connections
                    for (Proxy proxy : this.proxies) {
                        proxy.closeConnection();
                    }
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            this.addRemoveProxiesLock.unlock();
        }
    }

    private String sendRequest(Proxy proxy, String command, String body) throws IOException {
        Writer writer = proxy.getConnectionWriter();

        writer.append(command).append(NEW_LINE);

        if (body.length()>0)
            writer.append("Content-Length: ").append(String.valueOf(body.length())).append(NEW_LINE);
        writer.append("User-Agent: ClusterListener/1.0").append(NEW_LINE);
        writer.append("Connection: Keep-Alive").append(NEW_LINE);
        writer.write(NEW_LINE);
        if (body.length()>0) {
            writer.write(body);
        }
        writer.flush();

        // Read the first response line and skip the rest of the HTTP header
        return proxy.getConnectionReader().readLine();
    }

    private void appendParameter(Appendable appender, String name, String value, boolean more) throws IOException {
        appender.append(URLEncoder.encode(name, "UTF-8")).append('=').append(URLEncoder.encode(value, "UTF-8"));

        if (more) {
            appender.append('&');
        }
    }

    /**
     * Sends the given request to the given proxy, unless the proxy {@link Proxy.Lifecycle#accepts(MCMPRequestType) does not
     * accept} it in its current lifecycle state.
     *
     * @return the response body, or {@code null} if the request was rejected or failed
     */
    private String sendRequest(MCMPRequest request, Proxy proxy) {
        // Check and send while holding the proxy monitor, so that a concurrent handshake cannot interleave
        synchronized (proxy) {
            if (!proxy.getLifecycle().accepts(request.getRequestType())) {
                log.tracef("Not sending to %s in %s state: %s", proxy, proxy.getLifecycle(), request);
                return null;
            }

            return this.send(request, proxy).getBody();
        }
    }

    /**
     * Sends the given request to the given proxy regardless of its lifecycle state. On failure, the proxy is transitioned to
     * {@link Proxy.Lifecycle#ERROR} or {@link Proxy.Lifecycle#DOWN}; a successful response does not change its state.
     */
    private Response send(MCMPRequest request, Proxy proxy) {
        log.tracef("Sending to %s: %s", proxy, request);

        MCMPRequestType requestType = request.getRequestType();
        boolean wildcard = request.isWildcard();
        String jvmRoute = request.getJvmRoute();
        Map<String, String> parameters = request.getParameters();

        StringBuilder bodyBuilder = new StringBuilder();

        // First, encode the POST body
        try {
            if (jvmRoute != null) {
                this.appendParameter(bodyBuilder, "JVMRoute", jvmRoute, !parameters.isEmpty());
            }

            Iterator<Map.Entry<String, String>> entries = parameters.entrySet().iterator();

            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();

                this.appendParameter(bodyBuilder, entry.getKey(), entry.getValue(), entries.hasNext());
            }
        } catch (IOException e) {
            // Error encoding URL, should not happen
            throw new IllegalArgumentException(e);
        }

        // Then, connect to the proxy
        // Generate and write request
        StringBuilder headBuilder = new StringBuilder();

        headBuilder.append(requestType).append(" ");

        String proxyURL = this.config.getProxyURL();

        if (proxyURL != null) {
            headBuilder.append(proxyURL);
        }

        if (headBuilder.charAt(headBuilder.length() - 1) != '/') {
            headBuilder.append('/');
        }

        if (wildcard) {
            headBuilder.append('*');
        }

        headBuilder.append(" HTTP/1.1\r\n");
        headBuilder.append("Host: ");

        String head = headBuilder.toString();
        String body = bodyBuilder.toString();

        // Require exclusive access to proxy socket
        synchronized (proxy) {
            boolean success = false;
            try {
                String line = null;
                String host = proxy.getSocketAddress().getHostString();
                String proxyhead;

                if (host != null && host.contains(":")) {
                    proxyhead = head + "[" + host + "]:" + proxy.getSocketAddress().getPort();
                } else {
                    proxyhead = head + host + ":" + proxy.getSocketAddress().getPort();
                }

                try {
                    line = sendRequest(proxy, proxyhead, body);
                } catch (IOException e) {
                    // Ignore first write failure
                }

                if (line == null) {
                    // Retry failed read/write with fresh connection
                    proxy.closeConnection();
                    line = sendRequest(proxy, proxyhead, body);
                }

                BufferedReader reader = proxy.getConnectionReader();
                // Parse the line, which is formed like HTTP/1.x YYY Message
                int status = 500;
                // String version = "0";
                String message = null;
                String errorType = null;
                int contentLength = 0;
                boolean close = false;
                boolean chunked = false;
                if (line != null) {
                    try {
                        int spaceIndex = line.indexOf(' ');

                        /* Ignore everything until we have a HTTP headers */
                        while (spaceIndex == -1) {
                            line = reader.readLine();
                            if (line == null) {
                                // Connection closed without a response
                                proxy.setLifecycle(Proxy.Lifecycle.ERROR);
                                return Response.FAILED;
                            }
                            spaceIndex = line.indexOf(' ');
                        }
                        String responseStatus = line.substring(spaceIndex + 1, line.indexOf(' ', spaceIndex + 1));
                        status = Integer.parseInt(responseStatus);
                        line = reader.readLine();
                        while ((line != null) && (line.length() > 0)) {
                            int colon = line.indexOf(':');
                            String headerName = line.substring(0, colon).trim();
                            String headerValue = line.substring(colon + 1).trim();
                            if ("version".equalsIgnoreCase(headerName)) {
                                // version = headerValue;
                            } else if ("type".equalsIgnoreCase(headerName)) {
                                errorType = headerValue;
                            } else if ("mess".equalsIgnoreCase(headerName)) {
                                message = headerValue;
                            } else if ("content-length".equalsIgnoreCase(headerName)) {
                                contentLength = Integer.parseInt(headerValue);
                            } else if ("connection".equalsIgnoreCase(headerName)) {
                                close = "close".equalsIgnoreCase(headerValue);
                            } else if ("Transfer-Encoding".equalsIgnoreCase(headerName)) {
                                if ("chunked".equalsIgnoreCase(headerValue))
                                    chunked = true;
                            }
                            line = reader.readLine();
                        }
                    } catch (Exception e) {
                        ModClusterLogger.LOGGER.parseHeaderFailed(e, requestType, proxy.getSocketAddress());
                    }
                }

                // Mark as error if the front end server did not return 200; the configuration will
                // be refreshed during the next periodic event
                // n.b. a successful response never promotes the proxy; only a completed handshake does
                if (status == 200) {
                    success = true;

                    // REMOVE-APP * removes the node from the proxy, which thus needs to be configured again
                    if (requestType == MCMPRequestType.REMOVE_APP && wildcard && proxy.getLifecycle() == Proxy.Lifecycle.ESTABLISHED) {
                        proxy.setLifecycle(Proxy.Lifecycle.CONNECTED);
                    }
                } else {
                    if ("SYNTAX".equals(errorType)) {
                        // Syntax error means the protocol is incorrect, which cannot be automatically fixed
                        proxy.setLifecycle(Proxy.Lifecycle.DOWN);
                        ModClusterLogger.LOGGER.unrecoverableErrorResponse(errorType, requestType, proxy.getSocketAddress(), message);
                    } else {
                        proxy.setLifecycle(Proxy.Lifecycle.ERROR);
                        ModClusterLogger.LOGGER.recoverableErrorResponse(errorType, requestType, proxy.getSocketAddress(), message);
                    }
                }

                if (close) {
                    contentLength = Integer.MAX_VALUE;
                } else if (contentLength == 0 && ! chunked) {
                    return new Response(success, null);
                }

                // Read the request body
                StringBuilder result = new StringBuilder();
                char[] buffer = new char[512];

                if (chunked) {
                    boolean skipcrlf = false;
                    for (;;) {
                        if (skipcrlf) {
                            reader.readLine(); // Skip CRLF
                        } else {
                            skipcrlf = true;
                        }
                        line = reader.readLine();
                        contentLength = Integer.parseInt(line, 16);
                        if (contentLength == 0) {
                            reader.readLine(); // Skip last CRLF.
                            break;
                        }
                        while (contentLength > 0) {
                            int bytes = reader.read(buffer, 0, (contentLength > buffer.length) ? buffer.length : contentLength);
                            if (bytes <= 0) {
                                break;
                            }
                            result.append(buffer, 0, bytes);
                            contentLength -= bytes;
                        }
                    }
                } else {
                    while (contentLength > 0) {
                        int bytes = reader.read(buffer, 0, (contentLength > buffer.length) ? buffer.length : contentLength);
                        if (bytes <= 0) {
                            break;
                        }
                        result.append(buffer, 0, bytes);
                        contentLength -= bytes;
                    }
                }

                if (success) {
                    proxy.setIoExceptionLogged(false);
                }

                if (close) {
                    proxy.closeConnection();
                }

                return new Response(success, result.toString());
            } catch (IOException e) {
                success = false;

                // Most likely this is a connection error with the proxy
                proxy.setLifecycle(Proxy.Lifecycle.ERROR);

                // Log it only if we haven't done so already. Don't spam the log
                if (!proxy.isIoExceptionLogged()) {
                    ModClusterLogger.LOGGER.sendFailed(requestType, proxy.getSocketAddress(), e.getLocalizedMessage());
                    ModClusterLogger.LOGGER.catchingDebug(e);
                    proxy.setIoExceptionLogged(true);
                }

                return Response.FAILED;
            } finally {
                // If there's an error of any sort, or if the proxy did not return 200, it is an error
                if (!success) {
                    proxy.closeConnection();
                }
            }
        }
    }

    /**
     * Outcome of a single request sent to a proxy.
     */
    private static class Response {
        static final Response FAILED = new Response(false, null);

        private final boolean success;
        private final String body;

        Response(boolean success, String body) {
            this.success = success;
            this.body = body;
        }

        boolean isSuccess() {
            return this.success;
        }

        String getBody() {
            return this.body;
        }
    }

    /**
     * This class represents a front-end httpd server.
     */
    @ThreadSafe
    private static class Proxy implements MCMPServerState, Serializable {
        /** The serialVersionUID */
        private static final long serialVersionUID = 5219680414337319908L;

        private InetSocketAddress socketAddress;
        private final InetSocketAddress sourceAddress;

        /**
         * Lifecycle of the communication with a proxy, see {@code doc/design/proxy-lifecycle.md}.
         */
        enum Lifecycle {
            /** Not reachable or not yet contacted; only {@code INFO} is allowed, and the next status will handshake. */
            ERROR(State.ERROR, MCMPRequestType.INFO),
            /** Reachable ({@code INFO} succeeded), but the node has not yet been configured via {@code CONFIG}. */
            CONNECTED(State.ERROR, MCMPRequestType.INFO, MCMPRequestType.DUMP, MCMPRequestType.PING),
            /** Configured; all requests are allowed. */
            ESTABLISHED(State.OK, MCMPRequestType.values()),
            /** Unrecoverable error; requires an explicit {@link DefaultMCMPHandler#reset()}. */
            DOWN(State.DOWN, MCMPRequestType.INFO),
            ;

            private final State state;
            private final Set<MCMPRequestType> acceptedRequestTypes;

            Lifecycle(State state, MCMPRequestType... acceptedRequestTypes) {
                this.state = state;
                this.acceptedRequestTypes = EnumSet.copyOf(Arrays.asList(acceptedRequestTypes));
            }

            /**
             * @return the externally visible state
             */
            State getState() {
                return this.state;
            }

            /**
             * @return whether a request of the given type may be sent to a proxy in this lifecycle state
             */
            boolean accepts(MCMPRequestType requestType) {
                return this.acceptedRequestTypes.contains(requestType);
            }
        }

        private volatile Lifecycle lifecycle = Lifecycle.ERROR;

        private final transient int socketTimeout;
        private final transient SocketFactory socketFactory;

        private transient volatile boolean ioExceptionLogged = false;
        private transient volatile InetAddress localAddress = null;

        @GuardedBy("Proxy.this")
        private transient volatile Socket socket = null;
        @GuardedBy("Proxy.this")
        private transient volatile BufferedReader reader = null;
        @GuardedBy("Proxy.this")
        private transient volatile BufferedWriter writer = null;

        Proxy(InetSocketAddress socketAddress, MCMPHandlerConfiguration config) {
            this(socketAddress, null, config);
        }

        Proxy(InetSocketAddress socketAddress, InetSocketAddress sourceAddress, MCMPHandlerConfiguration config) {
            this.socketAddress = socketAddress;
            this.sourceAddress = sourceAddress;
            this.socketFactory = config.getSocketFactory();
            this.socketTimeout = config.getSocketTimeout();
        }

        // -------------------------------------------- MCMPServerState

        @Override
        public State getState() {
            return this.lifecycle.getState();
        }

        // ----------------------------------------------------------- MCMPServer

        @Override
        public InetSocketAddress getSocketAddress() {
            return this.socketAddress;
        }

        @Override
        public boolean isEstablished() {
            return this.lifecycle == Lifecycle.ESTABLISHED;
        }

        // ------------------------------------------------------------ Overrides

        @Override
        public String toString() {
            return this.socketAddress.toString();
        }

        @Override
        public boolean equals(Object object) {
            if ((object == null) || !(object instanceof MCMPServer))
                return false;

            MCMPServer proxy = (MCMPServer) object;

            return this.socketAddress.equals(proxy.getSocketAddress());
        }

        @Override
        public int hashCode() {
            return this.socketAddress.hashCode();
        }

        // -------------------------------------------------------------- Private

        Lifecycle getLifecycle() {
            return this.lifecycle;
        }

        void setLifecycle(Lifecycle lifecycle) {
            if (this.lifecycle != lifecycle) {
                log.tracef("Proxy %s transitioned from %s to %s", this, this.lifecycle, lifecycle);
                this.lifecycle = lifecycle;
            }
        }

        /**
         * Return a reader to the proxy.
         */
        private synchronized Socket getConnection() throws IOException {
            if (this.socket == null || this.socket.isClosed()) {
                this.socket = this.socketFactory.createSocket();

                // If a proxy is in ERROR or DOWN and has been configured with a hostname, as opposed to an IP address, attempt to resolve the address again.
                // Note that java.net.InetAddress.toString avoids a reverse name lookup.
                // Also note that JVM caches DNS name lookups.
                // See MODCLUSTER-728.
                if ((this.getState() != State.OK) && (this.socketAddress.isUnresolved() || this.socketAddress.getAddress().toString().indexOf('/') > 0)) {
                    this.socketAddress = new InetSocketAddress(this.socketAddress.getHostString(), this.socketAddress.getPort());
                }

                InetAddress address = this.socketAddress.getAddress();
                if (sourceAddress != null) {
                    // If using a specific port enable SO_REUSEADDR to avoid "Address already in use" errors
                    if (sourceAddress.getPort() != 0) {
                        this.socket.setReuseAddress(true);
                    }
                    // If bind address is specified for the proxy, use it
                    this.socket.bind(sourceAddress);
                } else if (address instanceof Inet6Address && address.isLinkLocalAddress()) {
                    // If the bind address is unspecified, workaround a JDK 6 IPv6 bug
                    InetSocketAddress bindAddr = new InetSocketAddress(address, 0);
                    this.socket.bind(bindAddr);
                }
                this.socket.connect(this.socketAddress, this.socketTimeout);
                this.socket.setSoTimeout(this.socketTimeout);
                this.localAddress = this.socket.getLocalAddress();
            }
            return this.socket;
        }

        /**
         * Convenience method that returns a reader to the proxy.
         */
        synchronized BufferedReader getConnectionReader() throws IOException {
            if (this.reader == null) {
                this.reader = new BufferedReader(new InputStreamReader(this.getConnection().getInputStream()));
            }
            return this.reader;
        }

        /**
         * Convenience method that returns a writer to the proxy.
         */
        synchronized BufferedWriter getConnectionWriter() throws IOException {
            if (this.writer == null) {
                this.writer = new BufferedWriter(new OutputStreamWriter(this.getConnection().getOutputStream()));
            }
            return this.writer;
        }

        InetAddress getLocalAddress() {
            return this.localAddress;
        }

        /**
         * Close connection.
         */
        synchronized void closeConnection() {
            if (this.reader != null) {
                try {
                    this.reader.close();
                } catch (IOException e) {
                    // Ignore
                }
                this.reader = null;
            }
            if (this.writer != null) {
                try {
                    this.writer.close();
                } catch (IOException e) {
                    // Ignore
                }
                this.writer = null;
            }
            if (this.socket != null) {
                if (!this.socket.isClosed()) {
                    try {
                        this.socket.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
                this.socket = null;
            }
        }

        boolean isIoExceptionLogged() {
            return this.ioExceptionLogged;
        }

        void setIoExceptionLogged(boolean ioErrorLogged) {
            this.ioExceptionLogged = ioErrorLogged;
        }
    }

    public static class VirtualHostImpl implements ResetRequestSource.VirtualHost, Externalizable {
        private final Set<String> aliases = new LinkedHashSet<String>();
        private final Map<String, ResetRequestSource.Status> contexts = new HashMap<String, ResetRequestSource.Status>();

        public VirtualHostImpl() {
            // Expose for deserialization
        }

        @Override
        public Set<String> getAliases() {
            return this.aliases;
        }

        @Override
        public Map<String, ResetRequestSource.Status> getContexts() {
            return this.contexts;
        }

        @Override
        public void readExternal(ObjectInput input) throws IOException {
            int aliases = input.readInt();
            for (int i = 0; i < aliases; ++i) {
                this.aliases.add(input.readUTF());
            }

            ResetRequestSource.Status[] stati = ResetRequestSource.Status.values();
            int contexts = input.readInt();

            for (int i = 0; i < contexts; ++i) {
                this.contexts.put(input.readUTF(), stati[input.readInt()]);
            }
        }

        @Override
        public void writeExternal(ObjectOutput output) throws IOException {
            output.writeInt(this.aliases.size());

            for (String alias : this.aliases) {
                output.writeUTF(alias);
            }

            output.writeInt(this.contexts.size());

            for (Map.Entry<String, ResetRequestSource.Status> context : this.contexts.entrySet()) {
                output.writeUTF(context.getKey());
                output.writeInt(context.getValue().ordinal());
            }
        }
    }
}
