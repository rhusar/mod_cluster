/*
 * Copyright The mod_cluster Project Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.jboss.modcluster;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.jboss.modcluster.config.MCMPHandlerConfiguration;
import org.jboss.modcluster.config.impl.SessionDrainingStrategyEnum;
import org.jboss.modcluster.container.Context;
import org.jboss.modcluster.container.Host;
import org.jboss.modcluster.container.Server;
import org.jboss.modcluster.load.LoadBalanceFactorProviderFactory;
import org.jboss.modcluster.mcmp.MCMPHandler;
import org.jboss.modcluster.mcmp.MCMPRequestFactory;
import org.jboss.modcluster.mcmp.ResetRequestSource;
import org.junit.jupiter.api.Test;

public class ModClusterServiceTest {

    private static final String SOME_PATH = "somePath";

    @Test
    void stopSingleContext() {

        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);
        Context context = createContext(SOME_PATH);
        addInnerContexts(context, context);

        mod.stop(context);

        verify(mcmpHandler, times(2)).sendRequest(eq(null));
    }

    @Test
    void stopMultipleContextWithSamePath() {

        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);
        Context context = createContext(SOME_PATH);
        Context context2 = createContext(SOME_PATH);
        addInnerContexts(context, context, context2);

        mod.stop(context);

        verify(mcmpHandler, never()).sendRequest(eq(null));
    }

    @Test
    void removeSingleContext() {

        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);
        Context context = createContext(SOME_PATH);
        addInnerContexts(context, context);

        mod.remove(context);

        verify(mcmpHandler, times(1)).sendRequest(eq(null));
    }

    @Test
    void removeMultipleContext() {

        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);
        Context context = createContext(SOME_PATH);
        Context context2 = createContext(SOME_PATH);
        addInnerContexts(context, context, context2);

        mod.remove(context);

        verify(mcmpHandler, never()).sendRequest(eq(null));
    }

    @Test
    void isEstablishedDelegatesToHandler() {
        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);

        // Connection established, but no proxy configured (yet), or no longer
        when(mcmpHandler.isEstablished()).thenReturn(false);
        assertFalse(mod.isEstablished());
        assertFalse(mod.enable());

        when(mcmpHandler.isEstablished()).thenReturn(true);
        assertTrue(mod.isEstablished());
    }

    @Test
    void startServerEstablishesProxies() {
        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);

        mod.start(setupServer());

        verify(mcmpHandler).establish();
        verify(mcmpHandler, never()).sendRequest(any());
    }

    @Test
    void resetAndRefreshWithoutEstablishedProxies() {
        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler);
        when(mcmpHandler.isEstablished()).thenReturn(false);

        mod.reset();
        mod.refresh();

        verify(mcmpHandler).reset();
        verify(mcmpHandler).markProxiesInError();
    }

    @Test
    void contextCommandsNotSentBeforeConnected() {
        MCMPHandler mcmpHandler = mock(MCMPHandler.class);
        ModClusterService mod = createModClusterService(mcmpHandler, false);
        Context context = createContext(SOME_PATH);
        addInnerContexts(context, context);

        mod.add(context);
        mod.start(context);
        mod.stop(context);
        mod.remove(context);

        verify(mcmpHandler, never()).sendRequest(any());
    }

    private ModClusterService createModClusterService(MCMPHandler mcmpHandler) {
        return createModClusterService(mcmpHandler, true);
    }

    private ModClusterService createModClusterService(MCMPHandler mcmpHandler, boolean connected) {
        ModClusterService modClusterService = new ModClusterService(
                null,
                null,
                setupMcmpConfig(),
                null,
                mock(LoadBalanceFactorProviderFactory.class),
                mock(MCMPRequestFactory.class),
                null,
                mock(ResetRequestSource.class),
                mcmpHandler,
                null);
        modClusterService.init(setupServer());
        if (connected) {
            modClusterService.connectionEstablished(null);
        }

        return modClusterService;
    }

    private Server setupServer() {
        Server server = mock(Server.class);
        when(server.getEngines()).thenReturn(new ArrayList<>());
        return server;
    }

    private MCMPHandlerConfiguration setupMcmpConfig() {
        MCMPHandlerConfiguration mcmpConfig = mock(MCMPHandlerConfiguration.class);
        when(mcmpConfig.getStopContextTimeoutUnit()).thenReturn(TimeUnit.SECONDS);
        when(mcmpConfig.getStopContextTimeout()).thenReturn(1L);
        when(mcmpConfig.getSessionDrainingStrategy()).thenReturn(SessionDrainingStrategyEnum.NEVER);
        when(mcmpConfig.getExcludedContextsPerHost()).thenReturn(new HashMap<>());
        when(mcmpConfig.getAdvertise()).thenReturn(false);
        return mcmpConfig;
    }

    private Context createContext(String somePath) {
        Context mock = mock(Context.class);
        when(mock.getPath()).thenReturn(somePath);
        return mock;
    }

    private void addInnerContexts(Context context, Context... innerContext) {
        when(setupHost(context).getContexts()).thenReturn(Arrays.asList(innerContext));
    }

    private Host setupHost(Context context) {
        Host host = mock(Host.class);
        when(context.getHost()).thenReturn(host);
        return host;
    }
}
