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

package org.elasticsearch.discovery;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.discovery.zen.fd.MasterFaultDetection;
import org.elasticsearch.discovery.zen.fd.NodesFaultDetection;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.local.LocalTransport;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class ZenFaultDetectionTests extends ESTestCase {

    protected ThreadPool threadPool;

    protected static final Version version0 = Version.fromId(/*0*/99);
    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;

    protected static final Version version1 = Version.fromId(199);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool(getClass().getName());
        serviceA = build(Settings.builder().put("name", "TS_A").build(), version0);
        nodeA = new DiscoveryNode("TS_A", "TS_A", serviceA.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version0);
        serviceB = build(Settings.builder().put("name", "TS_B").build(), version1);
        nodeB = new DiscoveryNode("TS_B", "TS_B", serviceB.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version1);

        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final CountDownLatch latch = new CountDownLatch(4);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);

        serviceA.connectToNode(nodeB);
        serviceA.connectToNode(nodeA);
        serviceB.connectToNode(nodeA);
        serviceB.connectToNode(nodeB);

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        serviceA.close();
        serviceB.close();
        terminate(threadPool);
    }

    protected MockTransportService build(Settings settings, Version version) {
        MockTransportService transportService = new MockTransportService(Settings.EMPTY, new LocalTransport(settings, threadPool, version, new NamedWriteableRegistry()), threadPool);
        transportService.start();
        return transportService;
    }

    private DiscoveryNodes buildNodesForA(boolean master) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.put(nodeA);
        builder.put(nodeB);
        builder.localNodeId(nodeA.id());
        builder.masterNodeId(master ? nodeA.id() : nodeB.id());
        return builder.build();
    }

    private DiscoveryNodes buildNodesForB(boolean master) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.put(nodeA);
        builder.put(nodeB);
        builder.localNodeId(nodeB.id());
        builder.masterNodeId(master ? nodeB.id() : nodeA.id());
        return builder.build();
    }

    @Test
    public void testNodesFaultDetectionConnectOnDisconnect() throws InterruptedException {
        Settings.Builder settings = Settings.builder();
        boolean shouldRetry = randomBoolean();
        // make sure we don't ping again after the initial ping
        settings.put(FaultDetection.SETTING_CONNECT_ON_NETWORK_DISCONNECT, shouldRetry)
                .put(FaultDetection.SETTING_PING_INTERVAL, "5m");
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(buildNodesForA(true)).build();
        NodesFaultDetection nodesFDA = new NodesFaultDetection(settings.build(), threadPool, serviceA, clusterState.getClusterName());
        nodesFDA.setLocalNode(nodeA);
        NodesFaultDetection nodesFDB = new NodesFaultDetection(settings.build(), threadPool, serviceB, clusterState.getClusterName());
        nodesFDB.setLocalNode(nodeB);
        final CountDownLatch pingSent = new CountDownLatch(1);
        nodesFDB.addListener(new NodesFaultDetection.Listener() {
            @Override
            public void onPingReceived(NodesFaultDetection.PingRequest pingRequest) {
                pingSent.countDown();
            }
        });
        nodesFDA.updateNodesAndPing(clusterState);

        // wait for the first ping to go out, so we will really respond to a disconnect event rather then
        // the ping failing
        pingSent.await(30, TimeUnit.SECONDS);

        final String[] failureReason = new String[1];
        final DiscoveryNode[] failureNode = new DiscoveryNode[1];
        final CountDownLatch notified = new CountDownLatch(1);
        nodesFDA.addListener(new NodesFaultDetection.Listener() {
            @Override
            public void onNodeFailure(DiscoveryNode node, String reason) {
                failureNode[0] = node;
                failureReason[0] = reason;
                notified.countDown();
            }
        });
        // will raise a disconnect on A
        serviceB.stop();
        notified.await(30, TimeUnit.SECONDS);

        assertEquals(nodeB, failureNode[0]);
        Matcher<String> matcher = Matchers.containsString("verified");
        if (!shouldRetry) {
            matcher = Matchers.not(matcher);
        }

        assertThat(failureReason[0], matcher);
    }

    @Test
    public void testMasterFaultDetectionConnectOnDisconnect() throws InterruptedException {

        Settings.Builder settings = Settings.builder();
        boolean shouldRetry = randomBoolean();
        // make sure we don't ping
        settings.put(FaultDetection.SETTING_CONNECT_ON_NETWORK_DISCONNECT, shouldRetry)
                .put(FaultDetection.SETTING_PING_INTERVAL, "5m");
        ClusterName clusterName = new ClusterName(randomAsciiOfLengthBetween(3, 20));
        final ClusterState state = ClusterState.builder(clusterName).nodes(buildNodesForA(false)).build();
        MasterFaultDetection masterFD = new MasterFaultDetection(settings.build(), threadPool, serviceA, clusterName,
                new NoopClusterService(state));
        masterFD.start(nodeB, "test");

        final String[] failureReason = new String[1];
        final DiscoveryNode[] failureNode = new DiscoveryNode[1];
        final CountDownLatch notified = new CountDownLatch(1);
        masterFD.addListener(new MasterFaultDetection.Listener() {

            @Override
            public void onMasterFailure(DiscoveryNode masterNode, String reason) {
                failureNode[0] = masterNode;
                failureReason[0] = reason;
                notified.countDown();
            }
        });
        // will raise a disconnect on A
        serviceB.stop();
        notified.await(30, TimeUnit.SECONDS);

        assertEquals(nodeB, failureNode[0]);
        Matcher<String> matcher = Matchers.containsString("verified");
        if (!shouldRetry) {
            matcher = Matchers.not(matcher);
        }

        assertThat(failureReason[0], matcher);
    }
}