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

package org.elasticsearch.discovery.zen;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests.AssertingAckListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.zen.ZenDiscovery.shouldIgnoreOrRejectNewClusterState;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;

public class ZenDiscoveryUnitTests extends ESTestCase {

    public void testShouldIgnoreNewClusterState() {
        ClusterName clusterName = new ClusterName("abc");

        DiscoveryNodes.Builder currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId("a").add(new DiscoveryNode("a", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));
        DiscoveryNodes.Builder newNodes = DiscoveryNodes.builder();
        newNodes.masterNodeId("a").add(new DiscoveryNode("a", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));

        ClusterState.Builder currentState = ClusterState.builder(clusterName);
        currentState.nodes(currentNodes);
        ClusterState.Builder newState = ClusterState.builder(clusterName);
        newState.nodes(newNodes);

        currentState.version(2);
        newState.version(1);
        assertTrue("should ignore, because new state's version is lower to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
        currentState.version(1);
        newState.version(1);
        assertTrue("should ignore, because new state's version is equal to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
        currentState.version(1);
        newState.version(2);
        assertFalse("should not ignore, because new state's version is higher to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));

        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId("b").add(new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT));

        // version isn't taken into account, so randomize it to ensure this.
        if (randomBoolean()) {
            currentState.version(2);
            newState.version(1);
        } else {
            currentState.version(1);
            newState.version(2);
        }
        currentState.nodes(currentNodes);
        try {
            shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build());
            fail("should ignore, because current state's master is not equal to new state's master");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cluster state from a different master than the current one, rejecting"));
        }

        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId(null);
        currentState.nodes(currentNodes);
        // version isn't taken into account, so randomize it to ensure this.
        if (randomBoolean()) {
            currentState.version(2);
            newState.version(1);
        } else {
            currentState.version(1);
            newState.version(2);
        }
        assertFalse("should not ignore, because current state doesn't have a master", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
    }

    public void testFilterNonMasterPingResponse() {
        ArrayList<ZenPing.PingResponse> responses = new ArrayList<>();
        ArrayList<DiscoveryNode> masterNodes = new ArrayList<>();
        ArrayList<DiscoveryNode> allNodes = new ArrayList<>();
        for (int i = randomIntBetween(10, 20); i >= 0; i--) {
            Set<Role> roles = new HashSet<>(randomSubsetOf(Arrays.asList(Role.values())));
            DiscoveryNode node = new DiscoveryNode("node_" + i, "id_" + i, LocalTransportAddress.buildUnique(), Collections.emptyMap(),
                    roles, Version.CURRENT);
            responses.add(new ZenPing.PingResponse(node, randomBoolean() ? null : node, new ClusterName("test"), randomLong()));
            allNodes.add(node);
            if (node.isMasterNode()) {
                masterNodes.add(node);
            }
        }

        boolean ignore = randomBoolean();
        List<ZenPing.PingResponse> filtered = ZenDiscovery.filterPingResponses(responses, ignore, logger);
        final List<DiscoveryNode> filteredNodes = filtered.stream().map(ZenPing.PingResponse::node).collect(Collectors.toList());
        if (ignore) {
            assertThat(filteredNodes, equalTo(masterNodes));
        } else {
            assertThat(filteredNodes, equalTo(allNodes));
        }
    }

    public void testNodesUpdatedAfterClusterStatePublished() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        // randomly make minimum_master_nodes a value higher than we have nodes for, so it will force failure
        int minMasterNodes = randomBoolean() ? 3 : 1;
        Settings settings = Settings.builder()
                                .put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.toString(minMasterNodes)).build();

        ArrayList<Closeable> toClose = new ArrayList<>();
        try {
            Set<DiscoveryNode> expectedFDNodes = null;

            final MockTransportService masterTransport = MockTransportService.local(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = new DiscoveryNode("master",  masterTransport.boundAddress().publishAddress(), Version.CURRENT);
            toClose.add(masterTransport);
            masterTransport.setLocalNode(masterNode);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, masterNode, masterNode);
            // build the zen discovery and cluster service
            ClusterService masterClusterService = createClusterService(threadPool, masterNode);
            toClose.add(masterClusterService);
            // TODO: clustername shouldn't be stored twice in cluster service, but for now, work around it
            state = ClusterState.builder(masterClusterService.getClusterName()).nodes(state.nodes()).build();
            setState(masterClusterService, state);
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterClusterService, threadPool);
            toClose.add(masterZen);
            masterTransport.acceptIncomingRequests();

            final MockTransportService otherTransport = MockTransportService.local(settings, Version.CURRENT, threadPool, null);
            otherTransport.start();
            toClose.add(otherTransport);
            DiscoveryNode otherNode = new DiscoveryNode("other", otherTransport.boundAddress().publishAddress(), Version.CURRENT);
            otherTransport.setLocalNode(otherNode);
            final ClusterState otherState = ClusterState.builder(masterClusterService.getClusterName())
                .nodes(DiscoveryNodes.builder().add(otherNode).localNodeId(otherNode.getId())).build();
            ClusterService otherClusterService = createClusterService(threadPool, masterNode);
            toClose.add(otherClusterService);
            setState(otherClusterService, otherState);
            ZenDiscovery otherZen = buildZenDiscovery(settings, otherTransport, otherClusterService, threadPool);
            toClose.add(otherZen);
            otherTransport.acceptIncomingRequests();

            masterTransport.connectToNode(otherNode);
            otherTransport.connectToNode(masterNode);

            // a new cluster state with a new discovery node (we will test if the cluster state
            // was updated by the presence of this node in NodesFaultDetection)
            ClusterState newState = ClusterState.builder(masterClusterService.state()).incrementVersion().nodes(
                DiscoveryNodes.builder(state.nodes()).add(otherNode).masterNodeId(masterNode.getId())
            ).build();

            try {
                // publishing a new cluster state
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent("testing", newState, state);
                AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
                expectedFDNodes = masterZen.getFaultDetectionNodes();
                masterZen.publish(clusterChangedEvent, listener);
                listener.await(1, TimeUnit.HOURS);
                // publish was a success, update expected FD nodes based on new cluster state
                expectedFDNodes = fdNodesForState(newState, masterNode);
            } catch (Discovery.FailedToCommitClusterStateException e) {
                // not successful, so expectedFDNodes above should remain what it was originally assigned
                assertEquals(3, minMasterNodes); // ensure min master nodes is the higher value, otherwise we shouldn't fail
            }

            assertEquals(expectedFDNodes, masterZen.getFaultDetectionNodes());
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }
    }

    public void testPendingCSQueueIsClearedWhenClusterStatePublished() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        // randomly make minimum_master_nodes a value higher than we have nodes for, so it will force failure
        int minMasterNodes =  randomBoolean() ? 3 : 1;
        Settings settings = Settings.builder()
            .put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.toString(minMasterNodes)).build();

        ArrayList<Closeable> toClose = new ArrayList<>();
        try {
            final MockTransportService masterTransport = MockTransportService.local(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = new DiscoveryNode("master",  masterTransport.boundAddress().publishAddress(), Version.CURRENT);
            toClose.add(masterTransport);
            masterTransport.setLocalNode(masterNode);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, null, masterNode);
            // build the zen discovery and cluster service
            ClusterService masterClusterService = createClusterService(threadPool, masterNode);
            toClose.add(masterClusterService);
            state = ClusterState.builder(masterClusterService.getClusterName()).nodes(state.nodes()).build();
            setState(masterClusterService, state);
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterClusterService, threadPool);
            toClose.add(masterZen);
            masterTransport.acceptIncomingRequests();

            // inject a pending cluster state
            masterZen.pendingClusterStatesQueue().addPending(ClusterState.builder(new ClusterName("foreign")).build());

            // a new cluster state with a new discovery node (we will test if the cluster state
            // was updated by the presence of this node in NodesFaultDetection)
            ClusterState newState = ClusterState.builder(masterClusterService.state()).incrementVersion().nodes(
                DiscoveryNodes.builder(masterClusterService.state().nodes()).masterNodeId(masterNode.getId())
            ).build();


            try {
                // publishing a new cluster state
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent("testing", newState, state);
                AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
                masterZen.publish(clusterChangedEvent, listener);
                listener.await(1, TimeUnit.HOURS);
                // publish was a success, check that queue as cleared
                assertThat(masterZen.pendingClusterStates(), emptyArray());
            } catch (Discovery.FailedToCommitClusterStateException e) {
                // not successful, so the pending queue should stay
                assertThat(masterZen.pendingClusterStates(), arrayWithSize(1));
                assertThat(masterZen.pendingClusterStates()[0].getClusterName().value(), equalTo("foreign"));
            }
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }
    }

    private ZenDiscovery buildZenDiscovery(Settings settings, TransportService service, ClusterService clusterService, ThreadPool threadPool) {
        ZenDiscovery zenDiscovery = new ZenDiscovery(settings, threadPool, service, clusterService, Collections::emptyList);
        zenDiscovery.start();
        return zenDiscovery;
    }

    private Set<DiscoveryNode> fdNodesForState(ClusterState clusterState, DiscoveryNode localNode) {
        final Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        clusterState.getNodes().getNodes().valuesIt().forEachRemaining(discoveryNode -> {
            // the local node isn't part of the nodes that are pinged (don't ping ourselves)
            if (discoveryNode.getId().equals(localNode.getId()) == false) {
                discoveryNodes.add(discoveryNode);
            }
        });
        return discoveryNodes;
    }
}
