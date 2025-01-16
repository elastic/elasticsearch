/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class TransportServiceHandshakeTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final TimeValue timeout = TimeValue.MAX_VALUE;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(TransportServiceHandshakeTests.class.getSimpleName());
    }

    private List<TransportService> transportServices = new ArrayList<>();

    private NetworkHandle startServices(String nodeNameAndId, Settings settings, Version version) {
        MockNioTransport transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );
        final DisruptingTransportInterceptor transportInterceptor = new DisruptingTransportInterceptor();
        TransportService transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            (boundAddress) -> new DiscoveryNode(
                nodeNameAndId,
                nodeNameAndId,
                boundAddress.publishAddress(),
                emptyMap(),
                emptySet(),
                version
            ),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transportServices.add(transportService);
        return new NetworkHandle(transportService, transportService.getLocalNode(), transportInterceptor);
    }

    @After
    public void tearDown() throws Exception {
        for (TransportService transportService : transportServices) {
            transportService.close();
        }
        super.tearDown();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        threadPool = null;
    }

    public void testConnectToNodeLight() throws IOException {
        Settings settings = Settings.builder().put("cluster.name", "test").build();

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices(
            "TS_B",
            settings,
            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT)
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
            DiscoveryNode connectedNode = PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut));
            assertNotNull(connectedNode);
            // the name and version should be updated
            assertEquals(connectedNode.getName(), "TS_B");
            assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
            assertFalse(handleA.transportService.nodeConnected(discoveryNode));
        }
    }

    public void testMismatchedClusterName() {

        NetworkHandle handleA = startServices("TS_A", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", Settings.builder().put("cluster.name", "b").build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut.map(x -> null)));
            }
        });
        assertThat(
            ex.getMessage(),
            containsString("handshake with [" + discoveryNode + "] failed: remote cluster name [b] does not match local cluster name [a]")
        );
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testIncompatibleVersions() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices(
            "TS_B",
            settings,
            VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion())
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut.map(x -> null)));
            }
        });
        assertThat(
            ex.getMessage(),
            containsString(
                "handshake with ["
                    + discoveryNode
                    + "] failed: remote node version ["
                    + handleB.discoveryNode.getVersion()
                    + "] is incompatible with local node version ["
                    + Version.CURRENT
                    + "]"
            )
        );
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testNodeConnectWithDifferentNodeId() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion()
        );
        ConnectTransportException ex = expectThrows(ConnectTransportException.class, () -> {
            handleA.transportService.connectToNode(discoveryNode, TestProfiles.LIGHT_PROFILE);
        });
        assertThat(ex.getMessage(), containsString("unexpected remote node"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testNodeConnectWithDifferentNodeIdSucceedsIfThisIsTransportClientOfSimpleNodeSampler() {
        Settings.Builder settings = Settings.builder().put("cluster.name", "test");
        Settings transportClientSettings = settings.put(Client.CLIENT_TYPE_SETTING_S.getKey(), TransportClient.CLIENT_TYPE).build();
        NetworkHandle handleA = startServices("TS_A", transportClientSettings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings.build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion()
        );

        handleA.transportService.connectToNode(discoveryNode, TestProfiles.LIGHT_PROFILE);
        assertTrue(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testNodeConnectWithDifferentNodeIdFailsWhenSnifferTransportClient() {
        Settings.Builder settings = Settings.builder().put("cluster.name", "test");
        Settings transportClientSettings = settings.put(Client.CLIENT_TYPE_SETTING_S.getKey(), TransportClient.CLIENT_TYPE)
            .put(TransportClient.CLIENT_TRANSPORT_SNIFF.getKey(), true)
            .build();
        NetworkHandle handleA = startServices("TS_A", transportClientSettings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings.build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion()
        );
        ConnectTransportException ex = expectThrows(ConnectTransportException.class, () -> {
            handleA.transportService.connectToNode(discoveryNode, TestProfiles.LIGHT_PROFILE);
        });
        assertThat(ex.getMessage(), containsString("unexpected remote node"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testRejectsMismatchedBuildHash() {
        final Settings settings = Settings.builder().put("cluster.name", "a").build();
        final NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        final NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT);
        final DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        handleA.transportInterceptor.setModifyBuildHash(true);
        handleB.transportInterceptor.setModifyBuildHash(true);
        TransportSerializationException ex = expectThrows(TransportSerializationException.class, () -> {
            try (Transport.Connection connection = handleA.transportService.openConnection(discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut.map(x -> null)));
            }
        });
        assertThat(
            ExceptionsHelper.unwrap(ex, IllegalArgumentException.class).getMessage(),
            containsString("which has an incompatible wire format")
        );
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testAcceptsMismatchedBuildHashFromDifferentVersion() {
        final NetworkHandle handleA = startServices("TS_A", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        final NetworkHandle handleB = startServices(
            "TS_B",
            Settings.builder().put("cluster.name", "a").build(),
            Version.CURRENT.minimumCompatibilityVersion()
        );
        handleA.transportInterceptor.setModifyBuildHash(true);
        handleB.transportInterceptor.setModifyBuildHash(true);
        handleA.transportService.connectToNode(handleB.discoveryNode, TestProfiles.LIGHT_PROFILE);
        assertTrue(handleA.transportService.nodeConnected(handleB.discoveryNode));
    }

    public void testAcceptsMismatchedBuildHashForTransportClient() {
        final NetworkHandle handleA = startServices(
            "TS_A",
            Settings.builder().put(Client.CLIENT_TYPE_SETTING_S.getKey(), TransportClient.CLIENT_TYPE).build(),
            Version.CURRENT
        );
        final NetworkHandle handleB = startServices("TS_B", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        handleA.transportInterceptor.setModifyBuildHash(true);
        handleB.transportInterceptor.setModifyBuildHash(true);
        handleA.transportService.connectToNode(handleB.discoveryNode, TestProfiles.LIGHT_PROFILE);
        assertTrue(handleA.transportService.nodeConnected(handleB.discoveryNode));
    }

    private static class NetworkHandle {
        final TransportService transportService;
        final DiscoveryNode discoveryNode;
        final DisruptingTransportInterceptor transportInterceptor;

        NetworkHandle(TransportService transportService, DiscoveryNode discoveryNode, DisruptingTransportInterceptor transportInterceptor) {
            this.transportService = transportService;
            this.discoveryNode = discoveryNode;
            this.transportInterceptor = transportInterceptor;
        }
    }

    private static class DisruptingTransportInterceptor implements TransportInterceptor {

        private boolean modifyBuildHash;

        public void setModifyBuildHash(boolean modifyBuildHash) {
            this.modifyBuildHash = modifyBuildHash;
        }

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
            String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler
        ) {

            if (TransportService.HANDSHAKE_ACTION_NAME.equals(action)) {
                return (request, channel, task) -> actualHandler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public String getChannelType() {
                        return channel.getChannelType();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) throws IOException {
                        assertThat(response, instanceOf(TransportService.HandshakeResponse.class));
                        if (modifyBuildHash) {
                            final TransportService.HandshakeResponse handshakeResponse = (TransportService.HandshakeResponse) response;
                            channel.sendResponse(
                                new TransportService.HandshakeResponse(
                                    handshakeResponse.getVersion(),
                                    handshakeResponse.getBuildHash() + "-modified",
                                    handshakeResponse.getDiscoveryNode(),
                                    handshakeResponse.getClusterName()
                                )
                            );
                        } else {
                            channel.sendResponse(response);
                        }
                    }

                    @Override
                    public void sendResponse(Exception exception) throws IOException {
                        channel.sendResponse(exception);

                    }
                }, task);
            } else {
                return actualHandler;
            }
        }
    }

}
