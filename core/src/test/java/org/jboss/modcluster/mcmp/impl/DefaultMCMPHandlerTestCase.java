/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster.mcmp.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.SocketFactory;

import org.jboss.modcluster.config.MCMPHandlerConfiguration;
import org.jboss.modcluster.config.ProxyConfiguration;
import org.jboss.modcluster.mcmp.MCMPConnectionListener;
import org.jboss.modcluster.mcmp.MCMPHandler;
import org.jboss.modcluster.mcmp.MCMPRequest;
import org.jboss.modcluster.mcmp.MCMPRequestType;
import org.jboss.modcluster.mcmp.MCMPServerState;
import org.jboss.modcluster.mcmp.ResetRequestSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the proxy lifecycle of {@link DefaultMCMPHandler}, in particular that context commands are never sent to a proxy
 * which has not been configured yet (MODCLUSTER-879).
 *
 * @author Radoslav Husar
 */
class DefaultMCMPHandlerTestCase {

    private static final long TIMEOUT_SECONDS = 10;
    // Literal address without a hostname, so that it is not re-resolved (e.g. to ::1), see MODCLUSTER-728
    private static final InetAddress LOOPBACK;

    static {
        try {
            LOOPBACK = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    private final MCMPHandlerConfiguration config = mock(MCMPHandlerConfiguration.class);
    private final ResetRequestSource resetRequestSource = mock(ResetRequestSource.class);
    private final MCMPConnectionListener connectionListener = mock(MCMPConnectionListener.class);
    private final List<FakeProxy> fakeProxies = new ArrayList<>();

    private final MCMPHandler handler = new DefaultMCMPHandler(this.config, this.resetRequestSource, new DefaultMCMPRequestFactory(), new DefaultMCMPResponseParser());

    DefaultMCMPHandlerTestCase() {
        when(this.config.getSocketFactory()).thenReturn(SocketFactory.getDefault());
        when(this.config.getSocketTimeout()).thenReturn((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        List<MCMPRequest> resetRequests = Arrays.asList(request(MCMPRequestType.CONFIG), request(MCMPRequestType.ENABLE_APP));
        when(this.resetRequestSource.getResetRequests(any())).thenReturn(resetRequests);
    }

    @AfterEach
    void cleanup() {
        this.handler.shutdown();
        for (FakeProxy proxy : this.fakeProxies) {
            proxy.close();
        }
    }

    @Test
    void notEstablishedBeforeHandshake() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);

        // Proxies are only contacted on the first periodic status
        assertFalse(this.handler.isEstablished());
        assertTrue(this.handler.sendRequest(request(MCMPRequestType.STOP_APP)).isEmpty());
        assertTrue(proxy.getCommands().isEmpty());
        verify(this.connectionListener, never()).connectionEstablished(any());
    }

    @Test
    void establishOnPeriodicStatus() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();

        assertTrue(this.handler.isEstablished());
        assertTrue(this.handler.isProxyHealthOK());
        assertEquals(MCMPServerState.State.OK, this.getProxyState().getState());
        assertTrue(this.getProxyState().isEstablished());
        verify(this.connectionListener).connectionEstablished(any());

        this.handler.sendRequest(request(MCMPRequestType.STOP_APP));

        assertEquals(Arrays.asList(MCMPRequestType.INFO, MCMPRequestType.CONFIG, MCMPRequestType.ENABLE_APP, MCMPRequestType.STOP_APP), proxy.getCommands());
        assertFalse(proxy.isMemErrorSent());
    }

    @Test
    void removeAllRequiresEstablishingAgain() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();
        assertTrue(this.handler.isEstablished());

        // e.g. ModClusterService.stop(Server) removes the node from the proxy
        MCMPRequest removeAll = request(MCMPRequestType.REMOVE_APP);
        when(removeAll.isWildcard()).thenReturn(true);
        this.handler.sendRequest(removeAll);
        proxy.forgetConfiguration();

        assertFalse(this.handler.isEstablished());
        assertEquals(MCMPServerState.State.ERROR, this.getProxyState().getState());

        // Context commands must not reach a proxy, which does not know the node
        int commands = proxy.getCommands().size();
        this.handler.sendRequest(request(MCMPRequestType.ENABLE_APP));
        assertEquals(commands, proxy.getCommands().size());

        // e.g. ModClusterService.start(Server)
        this.handler.establish();

        assertTrue(this.handler.isEstablished());
        assertEquals(Arrays.asList(MCMPRequestType.INFO, MCMPRequestType.CONFIG, MCMPRequestType.ENABLE_APP), proxy.getCommands().subList(commands, proxy.getCommands().size()));
        assertFalse(proxy.isMemErrorSent());
    }

    @Test
    void noConfigDoesNotEstablish() throws IOException {
        when(this.resetRequestSource.getResetRequests(any())).thenReturn(Collections.emptyList());
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();

        // Reachable, but without a CONFIG the node does not exist on the proxy
        assertFalse(this.handler.isEstablished());
        assertEquals(MCMPServerState.State.ERROR, this.getProxyState().getState());
        assertEquals(Collections.singletonList(MCMPRequestType.INFO), proxy.getCommands());
        verify(this.connectionListener).connectionEstablished(any());
    }

    @Test
    void requestDuringHandshakeIsDeferred() throws Exception {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);

        // Hold the CONFIG response, so that the handshake is in progress
        CountDownLatch configReceived = new CountDownLatch(1);
        CountDownLatch configRelease = new CountDownLatch(1);
        proxy.holdConfig(configReceived, configRelease);

        Thread handshake = new Thread(this.handler::status);
        handshake.start();

        assertTrue(configReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        Thread event = new Thread(() -> this.handler.sendRequest(request(MCMPRequestType.STOP_APP)));
        event.start();

        // The context command must wait for the handshake to complete
        awaitState(event, Thread.State.BLOCKED);
        assertFalse(proxy.getCommands().contains(MCMPRequestType.STOP_APP));

        configRelease.countDown();

        handshake.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        event.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        assertEquals(Arrays.asList(MCMPRequestType.INFO, MCMPRequestType.CONFIG, MCMPRequestType.ENABLE_APP, MCMPRequestType.STOP_APP), proxy.getCommands());
        assertFalse(proxy.isMemErrorSent());
        assertTrue(this.handler.isEstablished());
    }

    @Test
    void proxyLosingConfigurationIsReestablished() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();
        assertTrue(this.handler.isEstablished());

        // e.g. proxy restarted
        proxy.forgetConfiguration();

        this.handler.sendRequest(request(MCMPRequestType.STATUS));

        assertTrue(proxy.isMemErrorSent());
        assertFalse(this.handler.isEstablished());
        assertEquals(MCMPServerState.State.ERROR, this.getProxyState().getState());

        // Further context commands are not sent until the proxy is reconfigured
        int commands = proxy.getCommands().size();
        this.handler.sendRequest(request(MCMPRequestType.ENABLE_APP));
        assertEquals(commands, proxy.getCommands().size());

        this.handler.status();

        assertTrue(this.handler.isEstablished());
        List<MCMPRequestType> reestablished = proxy.getCommands().subList(commands, proxy.getCommands().size());
        assertEquals(Arrays.asList(MCMPRequestType.INFO, MCMPRequestType.CONFIG, MCMPRequestType.ENABLE_APP), reestablished);
    }

    @Test
    void markProxiesInError() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();
        assertTrue(this.handler.isEstablished());

        this.handler.markProxiesInError();

        assertFalse(this.handler.isEstablished());

        this.handler.status();

        assertTrue(this.handler.isEstablished());
    }

    @Test
    void removingLastProxyIsNotEstablished() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.singletonList(proxy.getConfiguration()), this.connectionListener);
        this.handler.status();
        assertTrue(this.handler.isEstablished());

        this.handler.removeProxy(proxy.getConfiguration().getRemoteAddress());
        this.handler.status();

        assertTrue(this.handler.getProxyStates().isEmpty());
        assertFalse(this.handler.isEstablished());
    }

    @Test
    void requestsOnlySentToEstablishedProxies() throws IOException {
        FakeProxy proxy = this.startProxy();
        ProxyConfiguration unreachable = proxyConfiguration(unusedPort());

        this.handler.init(Arrays.asList(proxy.getConfiguration(), unreachable), this.connectionListener);
        this.handler.status();

        assertTrue(this.handler.isEstablished());
        assertFalse(this.handler.isProxyHealthOK());

        Map<MCMPServerState, String> responses = this.handler.sendRequest(request(MCMPRequestType.STOP_APP));
        assertEquals(2, responses.size());
        assertEquals(MCMPRequestType.STOP_APP, proxy.getCommands().get(proxy.getCommands().size() - 1));
    }

    @Test
    @SuppressWarnings("deprecation")
    void deprecatedAddProxyDoesNotForceEstablished() throws IOException {
        FakeProxy proxy = this.startProxy();

        this.handler.init(Collections.emptyList(), this.connectionListener);
        this.handler.addProxy(proxy.getConfiguration(), true);

        // Without completing the handshake, the proxy must not be considered established
        assertFalse(this.handler.isEstablished());

        this.handler.status();

        assertTrue(this.handler.isEstablished());
    }

    private MCMPServerState getProxyState() {
        return this.handler.getProxyStates().iterator().next();
    }

    private FakeProxy startProxy() throws IOException {
        FakeProxy proxy = new FakeProxy();
        this.fakeProxies.add(proxy);
        return proxy;
    }

    private static MCMPRequest request(MCMPRequestType type) {
        MCMPRequest request = mock(MCMPRequest.class);
        when(request.getRequestType()).thenReturn(type);
        when(request.getJvmRoute()).thenReturn("node1");
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        return request;
    }

    private static ProxyConfiguration proxyConfiguration(int port) {
        ProxyConfiguration configuration = mock(ProxyConfiguration.class);
        when(configuration.getRemoteAddress()).thenReturn(new InetSocketAddress(LOOPBACK, port));
        return configuration;
    }

    private static int unusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, LOOPBACK)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitState(Thread thread, Thread.State state) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (thread.getState() != state) {
            assertTrue(System.nanoTime() < deadline, "Thread did not reach " + state);
            Thread.sleep(10);
        }
    }

    /**
     * Minimal MCMP proxy, which like mod_proxy_cluster rejects node commands with a MEM error until the node is configured.
     */
    private static class FakeProxy implements Runnable {
        private final ServerSocket serverSocket = new ServerSocket(0, 50, LOOPBACK);
        private final List<MCMPRequestType> commands = new CopyOnWriteArrayList<>();
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();
        private final AtomicBoolean memErrorSent = new AtomicBoolean();
        private volatile boolean configured = false;
        private volatile CountDownLatch configReceived;
        private volatile CountDownLatch configRelease;

        FakeProxy() throws IOException {
            Thread thread = new Thread(this, "fake-proxy-acceptor");
            thread.setDaemon(true);
            thread.start();
        }

        ProxyConfiguration getConfiguration() {
            return proxyConfiguration(this.serverSocket.getLocalPort());
        }

        List<MCMPRequestType> getCommands() {
            return new ArrayList<>(this.commands);
        }

        boolean isMemErrorSent() {
            return this.memErrorSent.get();
        }

        void forgetConfiguration() {
            this.configured = false;
        }

        void holdConfig(CountDownLatch received, CountDownLatch release) {
            this.configReceived = received;
            this.configRelease = release;
        }

        void close() {
            try {
                this.serverSocket.close();
            } catch (IOException e) {
                // Ignore
            }
            for (Socket socket : this.sockets) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }

        @Override
        public void run() {
            while (!this.serverSocket.isClosed()) {
                try {
                    Socket socket = this.serverSocket.accept();
                    this.sockets.add(socket);
                    Thread thread = new Thread(() -> this.serve(socket), "fake-proxy-connection");
                    thread.setDaemon(true);
                    thread.start();
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                 Writer writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) continue;

                    MCMPRequestType type = parseRequestType(line.substring(0, line.indexOf(' ')));
                    int contentLength = 0;
                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        if (line.regionMatches(true, 0, "Content-Length:", 0, 15)) {
                            contentLength = Integer.parseInt(line.substring(15).trim());
                        }
                    }
                    reader.skip(contentLength);

                    this.commands.add(type);

                    boolean success = true;
                    switch (type) {
                        case INFO:
                        case DUMP:
                        case PING:
                            break;
                        case CONFIG:
                            if (this.configReceived != null) {
                                this.configReceived.countDown();
                                this.configRelease.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                            }
                            this.configured = true;
                            break;
                        default:
                            success = this.configured;
                    }

                    if (success) {
                        writer.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                    } else {
                        this.memErrorSent.set(true);
                        writer.write("HTTP/1.1 500 ERROR\r\nType: MEM\r\nMess: Can't read node with \"node1\" JVMRoute\r\nContent-Length: 0\r\n\r\n");
                    }
                    writer.flush();
                }
            } catch (IOException e) {
                // Connection closed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private static MCMPRequestType parseRequestType(String command) {
            for (MCMPRequestType type : MCMPRequestType.values()) {
                if (type.toString().equals(command)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(command);
        }
    }
}
