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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests.AssertingAckListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.RoutingTableTests.updateActiveAllocations;
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
        currentNodes.masterNodeId("a").add(new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        DiscoveryNodes.Builder newNodes = DiscoveryNodes.builder();
        newNodes.masterNodeId("a").add(new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));

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
        currentNodes.masterNodeId("b").add(new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));

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
            DiscoveryNode node = new DiscoveryNode("node_" + i, "id_" + i, buildNewFakeTransportAddress(), Collections.emptyMap(),
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

        ArrayDeque<Closeable> toClose = new ArrayDeque<>();
        try {
            Set<DiscoveryNode> expectedFDNodes = null;

            final MockTransportService masterTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = masterTransport.getLocalNode();
            toClose.addFirst(masterTransport);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, masterNode, masterNode);
            // build the zen discovery and cluster service
            ClusterService masterClusterService = createClusterService(threadPool, masterNode);
            toClose.addFirst(masterClusterService);
            // TODO: clustername shouldn't be stored twice in cluster service, but for now, work around it
            state = ClusterState.builder(masterClusterService.getClusterName()).nodes(state.nodes()).build();
            setState(masterClusterService, state);
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterClusterService, threadPool);
            toClose.addFirst(masterZen);
            masterTransport.acceptIncomingRequests();

            final MockTransportService otherTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            otherTransport.start();
            toClose.addFirst(otherTransport);
            DiscoveryNode otherNode = otherTransport.getLocalNode();
            final ClusterState otherState = ClusterState.builder(masterClusterService.getClusterName())
                .nodes(DiscoveryNodes.builder().add(otherNode).localNodeId(otherNode.getId())).build();
            ClusterService otherClusterService = createClusterService(threadPool, masterNode);
            toClose.addFirst(otherClusterService);
            setState(otherClusterService, otherState);
            ZenDiscovery otherZen = buildZenDiscovery(settings, otherTransport, otherClusterService, threadPool);
            toClose.addFirst(otherZen);
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

        ArrayDeque<Closeable> toClose = new ArrayDeque<>();
        try {
            final MockTransportService masterTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = masterTransport.getLocalNode();
            toClose.addFirst(masterTransport);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, null, masterNode);
            // build the zen discovery and cluster service
            ClusterService masterClusterService = createClusterService(threadPool, masterNode);
            toClose.addFirst(masterClusterService);
            state = ClusterState.builder(masterClusterService.getClusterName()).nodes(state.nodes()).build();
            setState(masterClusterService, state);
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterClusterService, threadPool);
            toClose.addFirst(masterZen);
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
        ZenDiscovery zenDiscovery = new ZenDiscovery(settings, threadPool, service, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            clusterService, Collections::emptyList);
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

    public void testValidateOnUnsupportedIndexVersionCreated() throws Exception {
        final int iters = randomIntBetween(3, 10);
        for (int i = 0; i < iters; i++) {
            ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
            final DiscoveryNode otherNode = new DiscoveryNode("other_node", buildNewFakeTransportAddress(), emptyMap(),
                EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
            MembershipAction.ValidateJoinRequestRequestHandler request = new MembershipAction.ValidateJoinRequestRequestHandler();
            final boolean incompatible = randomBoolean();
            IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(Settings.builder()
                .put(SETTING_VERSION_CREATED, incompatible ? VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion())
                    : VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis()))
                .state(IndexMetaData.State.OPEN)
                .build();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetaData.getIndex());
            RoutingTable.Builder routing = new RoutingTable.Builder();
            routing.addAsNew(indexMetaData);
            final ShardId shardId = new ShardId("test", "_na_", 0);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

            final DiscoveryNode primaryNode = otherNode;
            indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, primaryNode.getId(), null, true,
                ShardRoutingState.INITIALIZING, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "getting there")));
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
            IndexMetaData updatedIndexMetaData = updateActiveAllocations(indexRoutingTable, indexMetaData);
            stateBuilder.metaData(MetaData.builder().put(updatedIndexMetaData, false).generateClusterUuidIfNeeded())
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build());
            if (incompatible) {
                IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
                    request.messageReceived(new MembershipAction.ValidateJoinRequest(stateBuilder.build()), null));
                assertEquals("index [test] version not supported: "
                    + VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion())
                    + " minimum compatible index version is: " + Version.CURRENT.minimumCompatibilityVersion(), ex.getMessage());
            } else {
                AtomicBoolean sendResponse = new AtomicBoolean(false);
                request.messageReceived(new MembershipAction.ValidateJoinRequest(stateBuilder.build()), new TransportChannel() {
                    @Override
                    public String action() {
                        return null;
                    }

                    @Override
                    public String getProfileName() {
                        return null;
                    }

                    @Override
                    public long getRequestId() {
                        return 0;
                    }

                    @Override
                    public String getChannelType() {
                        return null;
                    }

                    @Override
                    public void sendResponse(TransportResponse response) throws IOException {
                        sendResponse.set(true);
                    }

                    @Override
                    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {

                    }

                    @Override
                    public void sendResponse(Exception exception) throws IOException {

                    }
                });
                assertTrue(sendResponse.get());
            }
        }
    }
}
