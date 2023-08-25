/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;
import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.assertNumHandshakes;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SecurityNetty4ServerTransportAuthenticationTests extends ESTestCase {

    private ThreadPool threadPool;
    private Settings settings;
    private SecurityNetty4ServerTransport securityNetty4ServerTransport;
    private TransportService serverTransportService;
    private DiscoveryNode serverNode;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        settings = Settings.builder()
            .put("node.name", "SecurityNetty4HeaderSizeLimitTests")
            .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED.getKey(), "false")
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), "true")
            .build();
        securityNetty4ServerTransport = new SecurityNetty4ServerTransport(
            settings,
            TransportVersion.current(),
            threadPool,
            new NetworkService(List.of()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(List.of()),
            new NoneCircuitBreakerService(),
            null,
            mock(SSLService.class),
            new SharedGroupFactory(settings),
            mock(CrossClusterAccessAuthenticationService.class)
        );
        serverTransportService = MockTransportService.createNewService(
            settings,
            securityNetty4ServerTransport,
            VersionInformation.CURRENT,
            threadPool,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            Collections.emptySet(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR // TODO maybe use transport interceptor to set credentials
        );
        serverTransportService.start();
        serverTransportService.acceptIncomingRequests();
        // TODO maybe use connection listeners
        serverNode = serverTransportService.getLocalNode();
    }

    // BUSTED
    public void testConnectWithRejectingAuthenticator() throws InterruptedException {
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                connectedLatch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                fail("disconnect should not be called " + node);
            }
        };
        serverTransportService.addConnectionListener(waitForConnection);
        try (
            MockTransportService clientService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            clientService.start();
            AbstractSimpleTransportTestCase.connectToNode(clientService, serverNode);
            assertThat("failed to wait for node to connect", connectedLatch.await(5, TimeUnit.SECONDS), equalTo(true));
            assertNumHandshakes(1, securityNetty4ServerTransport);
        }
    }
}
