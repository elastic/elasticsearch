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
    private static ThreadPool THREAD_POOL;

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new ThreadPool(NettyTransportServiceHandshakeTests.class.getSimpleName());
    }

    private List<TransportService> transportServices = new ArrayList<>();

    private NetworkHandle startServices(String nodeNameAndId, Settings settings, Version version, ClusterName clusterName) {
        NettyTransport transport =
                new NettyTransport(
                        settings,
                        THREAD_POOL,
                        new NetworkService(settings),
                        BigArrays.NON_RECYCLING_INSTANCE,
                        Version.CURRENT,
                        new NamedWriteableRegistry(),
                        new NoneCircuitBreakerService());
        TransportService transportService = new MockTransportService(settings, transport, THREAD_POOL, clusterName);
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
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testConnectToNodeLight() {
        Settings settings = Settings.EMPTY;

        ClusterName test = new ClusterName("test");

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT, test);
        NetworkHandle handleB =
                startServices(
                        "TS_B",
                        settings,
                        VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT),
                        test);

        DiscoveryNode connectedNode =
                handleA.transportService.connectToNodeLight(
                        new DiscoveryNode(
                                "",
                                handleB.discoveryNode.getAddress(),
                                emptyMap(),
                                emptySet(),
                                Version.CURRENT.minimumCompatibilityVersion()),
                        100);
        assertNotNull(connectedNode);

        // the name and version should be updated
        assertEquals(connectedNode.getName(), "TS_B");
        assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
    }

    public void testMismatchedClusterName() {
        Settings settings = Settings.EMPTY;

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT, new ClusterName("a"));
        NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT, new ClusterName("b"));

        try {
            handleA.transportService.connectToNodeLight(
                    new DiscoveryNode(
                            "",
                            handleB.discoveryNode.getAddress(),
                            emptyMap(),
                            emptySet(),
                            Version.CURRENT.minimumCompatibilityVersion()),
                    100);
            fail("expected handshake to fail from mismatched cluster names");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("handshake failed, mismatched cluster name [Cluster [b]]"));
        }
    }

    public void testIncompatibleVersions() {
        Settings settings = Settings.EMPTY;

        ClusterName test = new ClusterName("test");
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT, test);
        NetworkHandle handleB =
                startServices("TS_B", settings, VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()), test);

        try {
            handleA.transportService.connectToNodeLight(
                    new DiscoveryNode(
                            "",
                            handleB.discoveryNode.getAddress(),
                            emptyMap(),
                            emptySet(),
                            Version.CURRENT.minimumCompatibilityVersion()),
                    100);
            fail("expected handshake to fail from incompatible versions");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("handshake failed, incompatible version"));
        }
    }

    public void testIgnoreMismatchedClusterName() {
        Settings settings = Settings.EMPTY;

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT, new ClusterName("a"));
        NetworkHandle handleB =
                startServices(
                        "TS_B",
                        settings,
                        VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT),
                        new ClusterName("b")
                );

        DiscoveryNode connectedNode = handleA.transportService.connectToNodeLight(
                new DiscoveryNode(
                        "",
                        handleB.discoveryNode.getAddress(),
                        emptyMap(),
                        emptySet(),
                        Version.CURRENT.minimumCompatibilityVersion()),
                100,
                false);
        assertNotNull(connectedNode);
        assertEquals(connectedNode.getName(), "TS_B");
        assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
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
