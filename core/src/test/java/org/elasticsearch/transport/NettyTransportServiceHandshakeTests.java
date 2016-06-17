/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;

public class NettyTransportServiceHandshakeTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final long timeout = Long.MAX_VALUE;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(NettyTransportServiceHandshakeTests.class.getSimpleName());
    }

    private List<TransportService> transportServices = new ArrayList<>();

    private NetworkHandle startServices(String nodeNameAndId, Settings settings, Version version) {
        NettyTransport transport =
                new NettyTransport(
                        settings,
                        threadPool,
                        new NetworkService(settings),
                        BigArrays.NON_RECYCLING_INSTANCE,
                        Version.CURRENT,
                        new NamedWriteableRegistry(),
                        new NoneCircuitBreakerService());
        TransportService transportService = new MockTransportService(settings, transport, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        DiscoveryNode node =
                new DiscoveryNode(
                        nodeNameAndId,
                        nodeNameAndId,
                        transportService.boundAddress().publishAddress(),
                        emptyMap(),
                        emptySet(),
                        version);
        transportService.setLocalNode(node);
        transportServices.add(transportService);
        return new NetworkHandle(transportService, node);
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

    public void testConnectToNodeLight() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB =
                startServices(
                        "TS_B",
                        settings,
                        VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT));
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        DiscoveryNode connectedNode =
                handleA.transportService.connectToNodeLightAndHandshake(discoveryNode, timeout);
        assertNotNull(connectedNode);

        // the name and version should be updated
        assertEquals(connectedNode.getName(), "TS_B");
        assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
        assertTrue(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testMismatchedClusterName() {

        NetworkHandle handleA = startServices("TS_A", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", Settings.builder().put("cluster.name", "b").build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handleA.transportService.connectToNodeLightAndHandshake(
                discoveryNode, timeout));
        assertThat(ex.getMessage(), containsString("handshake failed, mismatched cluster name [Cluster [b]]"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
}

    public void testIncompatibleVersions() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB =
                startServices("TS_B", settings, VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()));
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handleA.transportService.connectToNodeLightAndHandshake(
            discoveryNode, timeout));
        assertThat(ex.getMessage(), containsString("handshake failed, incompatible version"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testIgnoreMismatchedClusterName() {
        Settings settings = Settings.builder().put("cluster.name", "a").build();

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB =
                startServices(
                        "TS_B",
                        Settings.builder().put("cluster.name", "b").build(),
                        VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT)
                );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        DiscoveryNode connectedNode = handleA.transportService.connectToNodeLightAndHandshake(discoveryNode, timeout, false);
        assertNotNull(connectedNode);
        assertEquals(connectedNode.getName(), "TS_B");
        assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
        assertTrue(handleA.transportService.nodeConnected(discoveryNode));
    }

    private static class NetworkHandle {
        private TransportService transportService;
        private DiscoveryNode discoveryNode;

        public NetworkHandle(TransportService transportService, DiscoveryNode discoveryNode) {
            this.transportService = transportService;
            this.discoveryNode = discoveryNode;
        }
    }

}
