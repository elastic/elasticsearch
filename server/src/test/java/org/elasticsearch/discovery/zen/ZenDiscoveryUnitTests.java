/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.coordination.JoinTaskExecutor;
import org.elasticsearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.elasticsearch.cluster.coordination.ValidateJoinRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests.AssertingAckListener;
import org.elasticsearch.discovery.zen.ZenDiscovery.ZenNodeRemovalClusterStateTaskExecutor;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.RoutingTableTests.updateActiveAllocations;
import static org.elasticsearch.cluster.service.MasterServiceTests.discoveryState;
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.zen.ZenDiscovery.shouldIgnoreOrRejectNewClusterState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        ClusterState currentCs = currentState.build();
        ClusterState newCs = newState.build();
        assertTrue("should ignore, because new state's version is lower to current state's version",
            shouldIgnoreOrRejectNewClusterState(logger, currentCs, newCs));

        currentState = ClusterState.builder(currentCs).version(1);
        newState = ClusterState.builder(newCs).version(1);
        currentCs = currentState.build();
        newCs = newState.build();
        assertTrue("should ignore, because new state's version is equal to current state's version",
            shouldIgnoreOrRejectNewClusterState(logger, currentCs, newCs));
        currentState = ClusterState.builder(currentCs).version(1);
        newState = ClusterState.builder(newCs).version(2);
        currentCs = currentState.build();
        newCs = newState.build();
        assertFalse("should not ignore, because new state's version is higher to current state's version",
            shouldIgnoreOrRejectNewClusterState(logger, currentCs, newCs));

        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId("b").add(new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));

        // version isn't taken into account, so randomize it to ensure this.
        if (randomBoolean()) {
            currentState = ClusterState.builder(currentCs).version(2);
            newState = ClusterState.builder(newCs).version(1);
        } else {
            currentState = ClusterState.builder(currentCs).version(1);
            newState = ClusterState.builder(newCs).version(2);
        }
        currentState.nodes(currentNodes);
        currentCs = currentState.build();
        newCs = newState.build();
        try {
            shouldIgnoreOrRejectNewClusterState(logger, currentCs, newCs);
            fail("should ignore, because current state's master is not equal to new state's master");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cluster state from a different master than the current one, rejecting"));
        }

        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId(null);
        currentState = ClusterState.builder(currentCs).nodes(currentNodes);
        // version isn't taken into account, so randomize it to ensure this.
        if (randomBoolean()) {
            currentState.version(2);
            newState = ClusterState.builder(newCs).version(1);
        } else {
            currentState.version(1);
            newState = ClusterState.builder(newCs).version(2);
        }
        assertFalse("should not ignore, because current state doesn't have a master",
            shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
    }

    public void testFilterNonMasterPingResponse() {
        ArrayList<ZenPing.PingResponse> responses = new ArrayList<>();
        ArrayList<DiscoveryNode> masterNodes = new ArrayList<>();
        ArrayList<DiscoveryNode> allNodes = new ArrayList<>();
        for (int i = randomIntBetween(10, 20); i >= 0; i--) {
            Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
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
            // build the zen discovery and discovery service
            MasterService masterMasterService = ClusterServiceUtils.createMasterService(threadPool, masterNode);
            toClose.addFirst(masterMasterService);
            // TODO: clustername shouldn't be stored twice in cluster service, but for now, work around it
            state = ClusterState.builder(discoveryState(masterMasterService).getClusterName()).nodes(state.nodes()).build();
            Settings settingsWithClusterName = Settings.builder().put(settings).put(
                ClusterName.CLUSTER_NAME_SETTING.getKey(), discoveryState(masterMasterService).getClusterName().value()).build();
            ZenDiscovery masterZen = buildZenDiscovery(
                settingsWithClusterName,
                masterTransport, masterMasterService, threadPool);
            masterZen.setCommittedState(state);
            toClose.addFirst(masterZen);
            masterTransport.acceptIncomingRequests();

            final MockTransportService otherTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            otherTransport.start();
            toClose.addFirst(otherTransport);

            DiscoveryNode otherNode = otherTransport.getLocalNode();
            final ClusterState otherState = ClusterState.builder(discoveryState(masterMasterService).getClusterName())
                .nodes(DiscoveryNodes.builder().add(otherNode).localNodeId(otherNode.getId())).build();
            MasterService otherMasterService = ClusterServiceUtils.createMasterService(threadPool, otherNode);
            toClose.addFirst(otherMasterService);
            ZenDiscovery otherZen = buildZenDiscovery(settingsWithClusterName, otherTransport, otherMasterService, threadPool);
            otherZen.setCommittedState(otherState);
            toClose.addFirst(otherZen);
            otherTransport.acceptIncomingRequests();

            masterTransport.connectToNode(otherNode);
            otherTransport.connectToNode(masterNode);

            // a new cluster state with a new discovery node (we will test if the cluster state
            // was updated by the presence of this node in NodesFaultDetection)
            ClusterState newState = ClusterState.builder(discoveryState(masterMasterService)).incrementVersion().nodes(
                DiscoveryNodes.builder(state.nodes()).add(otherNode).masterNodeId(masterNode.getId())
            ).build();

            // publishing a new cluster state
            ClusterStatePublicationEvent clusterStatePublicationEvent
                = new ClusterStatePublicationEvent("testing", state, newState, 0L, 0L);
            AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
            expectedFDNodes = masterZen.getFaultDetectionNodes();
            AwaitingPublishListener awaitingPublishListener = new AwaitingPublishListener();
            masterZen.publish(clusterStatePublicationEvent, awaitingPublishListener, listener);
            awaitingPublishListener.await();
            if (awaitingPublishListener.getException() == null) {
                // publication succeeded, wait for acks
                listener.await(10, TimeUnit.SECONDS);
                // publish was a success, update expected FD nodes based on new cluster state
                expectedFDNodes = fdNodesForState(newState, masterNode);
            } else {
                // not successful, so expectedFDNodes above should remain what it was originally assigned
                assertEquals(3, minMasterNodes); // ensure min master nodes is the higher value, otherwise we shouldn't fail
            }

            assertEquals(expectedFDNodes, masterZen.getFaultDetectionNodes());
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }

        assertWarnings("[discovery.zen.minimum_master_nodes] setting was deprecated in Elasticsearch and will be removed in a future " +
            "release! See the breaking changes documentation for the next major version.");
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
            // build the zen discovery and master service for the master node
            MasterService masterMasterService = ClusterServiceUtils.createMasterService(threadPool, masterNode);
            toClose.addFirst(masterMasterService);
            state = ClusterState.builder(discoveryState(masterMasterService).getClusterName()).nodes(state.nodes()).build();
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterMasterService, threadPool);
            masterZen.setCommittedState(state);
            toClose.addFirst(masterZen);
            masterTransport.acceptIncomingRequests();

            // inject a pending cluster state
            masterZen.pendingClusterStatesQueue().addPending(ClusterState.builder(new ClusterName("foreign")).build());

            // a new cluster state with a new discovery node (we will test if the cluster state
            // was updated by the presence of this node in NodesFaultDetection)
            ClusterState newState = ClusterState.builder(discoveryState(masterMasterService)).incrementVersion().nodes(
                DiscoveryNodes.builder(discoveryState(masterMasterService).nodes()).masterNodeId(masterNode.getId())
            ).build();

            // publishing a new cluster state
            ClusterStatePublicationEvent clusterStatePublicationEvent
                = new ClusterStatePublicationEvent("testing", state, newState, 0L, 0L);
            AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
            AwaitingPublishListener awaitingPublishListener = new AwaitingPublishListener();
            masterZen.publish(clusterStatePublicationEvent, awaitingPublishListener, listener);
            awaitingPublishListener.await();
            if (awaitingPublishListener.getException() == null) {
                // publication succeeded, wait for acks
                listener.await(1, TimeUnit.HOURS);
            }
            // queue should be cleared whether successful or not
            assertThat(Arrays.toString(masterZen.pendingClusterStates()), masterZen.pendingClusterStates(), emptyArray());
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }

        assertWarnings("[discovery.zen.minimum_master_nodes] setting was deprecated in Elasticsearch and will be removed in a future " +
            "release! See the breaking changes documentation for the next major version.");
    }

    private class AwaitingPublishListener implements ActionListener<Void> {
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private FailedToCommitClusterStateException exception;

        @Override
        public synchronized void onResponse(Void aVoid) {
            assertThat(countDownLatch.getCount(), is(1L));
            countDownLatch.countDown();
        }

        @Override
        public synchronized void onFailure(Exception e) {
            assertThat(e, IsInstanceOf.instanceOf(FailedToCommitClusterStateException.class));
            exception = (FailedToCommitClusterStateException) e;
            onResponse(null);
        }

        public void await() throws InterruptedException {
            countDownLatch.await();
        }

        public synchronized FailedToCommitClusterStateException getException() {
            return exception;
        }
    }

    private ZenDiscovery buildZenDiscovery(Settings settings, TransportService service, MasterService masterService,
                                           ThreadPool threadPool) {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterApplier clusterApplier = new ClusterApplier() {
            @Override
            public void setInitialState(ClusterState initialState) {

            }

            @Override
            public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
                listener.onSuccess(source);
            }
        };
        ZenDiscovery zenDiscovery = new ZenDiscovery(settings, threadPool, service,
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            masterService, clusterApplier, clusterSettings, hostsResolver -> Collections.emptyList(),
            ESAllocationTestCase.createAllocationService(), Collections.emptyList(), (s, p, r) -> {});
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
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
            final DiscoveryNode localNode = new DiscoveryNode("other_node", buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
            MembershipAction.ValidateJoinRequestRequestHandler request = new MembershipAction.ValidateJoinRequestRequestHandler
                (() -> localNode, JoinTaskExecutor.addBuiltInJoinValidators(Collections.emptyList()));
            final boolean incompatible = randomBoolean();
            IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(Settings.builder()
                .put(SETTING_VERSION_CREATED,
                    incompatible ? VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion())
                        : VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis()))
                .state(IndexMetadata.State.OPEN)
                .build();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            RoutingTable.Builder routing = new RoutingTable.Builder();
            routing.addAsNew(indexMetadata);
            final ShardId shardId = new ShardId("test", "_na_", 0);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

            final DiscoveryNode primaryNode = otherNode;
            indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, primaryNode.getId(), null, true,
                ShardRoutingState.INITIALIZING, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "getting there")));
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
            IndexMetadata updatedIndexMetadata = updateActiveAllocations(indexRoutingTable, indexMetadata);
            stateBuilder.metadata(Metadata.builder().put(updatedIndexMetadata, false).generateClusterUuidIfNeeded())
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build());
            if (incompatible) {
                IllegalStateException ex = expectThrows(IllegalStateException.class, () ->
                    request.messageReceived(new ValidateJoinRequest(stateBuilder.build()), null, null));
                assertEquals("index [test] version not supported: "
                    + VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion())
                    + " minimum compatible index version is: " + Version.CURRENT.minimumIndexCompatibilityVersion(), ex.getMessage());
            } else {
                AtomicBoolean sendResponse = new AtomicBoolean(false);
                request.messageReceived(new ValidateJoinRequest(stateBuilder.build()), new TransportChannel() {

                    @Override
                    public String getProfileName() {
                        return null;
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
                    public void sendResponse(Exception exception) throws IOException {

                    }
                }, null);
                assertTrue(sendResponse.get());
            }
        }
    }

    public void testIncomingClusterStateValidation() throws Exception {
        ClusterName clusterName = new ClusterName("abc");

        DiscoveryNodes.Builder currentNodes = DiscoveryNodes.builder().add(
            new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)).localNodeId("a");

        ClusterState previousState = ClusterState.builder(clusterName).nodes(currentNodes).build();

        logger.info("--> testing acceptances of any master when having no master");
        ClusterState state = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId(randomAlphaOfLength(10))).incrementVersion().build();
        ZenDiscovery.validateIncomingState(logger, state, previousState);

        // now set a master node
        previousState = state;
        state = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("master")).build();
        logger.info("--> testing rejection of another master");
        try {
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state from another master");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("cluster state from a different master than the current one, rejecting"));
        }

        logger.info("--> test state from the current master is accepted");
        previousState = state;
        ZenDiscovery.validateIncomingState(logger, ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("master")).incrementVersion().build(), previousState);


        logger.info("--> testing rejection of another cluster name");
        try {
            ZenDiscovery.validateIncomingState(logger, ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
                .nodes(previousState.nodes()).build(), previousState);
            fail("node accepted state with another cluster name");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state from a node that is not part of the cluster"));
        }

        logger.info("--> testing rejection of a cluster state with wrong local node");
        try {
            state = ClusterState.builder(previousState)
                .nodes(DiscoveryNodes.builder(previousState.nodes()).localNodeId("_non_existing_").build())
                .incrementVersion().build();
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state with non-existence local node");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state with a local node that does not match the current local node"));
        }

        try {
            DiscoveryNode otherNode = new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            state = ClusterState.builder(previousState).nodes(
                DiscoveryNodes.builder(previousState.nodes()).add(otherNode)
                    .localNodeId(otherNode.getId()).build()
            ).incrementVersion().build();
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state with existent but wrong local node");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state with a local node that does not match the current local node"));
        }

        logger.info("--> testing acceptance of an old cluster state");
        final ClusterState incomingState = previousState;
        previousState = ClusterState.builder(previousState).incrementVersion().build();
        final ClusterState finalPreviousState = previousState;
        final IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> ZenDiscovery.validateIncomingState(logger, incomingState, finalPreviousState));
        final String message = String.format(
            Locale.ROOT,
            "rejecting cluster state version [%d] uuid [%s] received from [%s]",
            incomingState.version(),
            incomingState.stateUUID(),
            incomingState.nodes().getMasterNodeId()
        );
        assertThat(e, hasToString("java.lang.IllegalStateException: " + message));

        ClusterState higherVersionState = ClusterState.builder(previousState).incrementVersion().build();
        // remove the master of the node (but still have a previous cluster state with it)!
        higherVersionState = ClusterState.builder(higherVersionState)
            .nodes(DiscoveryNodes.builder(higherVersionState.nodes()).masterNodeId(null)).build();
        // an older version from a *new* master is also OK!
        state = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("_new_master_").build())
            .build();

        ZenDiscovery.validateIncomingState(logger, state, higherVersionState);
    }

    @SuppressWarnings("unchecked")
    public void testNotEnoughMasterNodesAfterRemove() throws Exception {
        final ElectMasterService electMasterService = mock(ElectMasterService.class);
        when(electMasterService.hasEnoughMasterNodes(any(Iterable.class))).thenReturn(false);

        final AllocationService allocationService = mock(AllocationService.class);

        final AtomicBoolean rejoinCalled = new AtomicBoolean();
        final Consumer<String> submitRejoin = source -> rejoinCalled.set(true);

        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        final ZenNodeRemovalClusterStateTaskExecutor executor =
            new ZenNodeRemovalClusterStateTaskExecutor(allocationService, electMasterService, submitRejoin, logger) {
                @Override
                protected ClusterState remainingNodesClusterState(ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
                    remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder));
                    return remainingNodesClusterState.get();
                }
            };

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int nodes = randomIntBetween(2, 16);
        final List<NodeRemovalClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        // to ensure there is at least one removal
        boolean first = true;
        for (int i = 0; i < nodes; i++) {
            final DiscoveryNode node = node(i);
            builder.add(node);
            if (first || randomBoolean()) {
                tasks.add(new NodeRemovalClusterStateTaskExecutor.Task(node, randomBoolean() ? "left" : "failed"));
            }
            first = false;
        }
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(builder).build();

        final ClusterStateTaskExecutor.ClusterTasksResult<NodeRemovalClusterStateTaskExecutor.Task> result =
            executor.execute(clusterState, tasks);
        verify(electMasterService).hasEnoughMasterNodes(eq(remainingNodesClusterState.get().nodes()));
        verify(electMasterService).countMasterNodes(eq(remainingNodesClusterState.get().nodes()));
        verify(electMasterService).minimumMasterNodes();
        verifyNoMoreInteractions(electMasterService);

        // ensure that we did not reroute
        verifyNoMoreInteractions(allocationService);
        assertTrue(rejoinCalled.get());
        assertThat(result.resultingState, CoreMatchers.equalTo(clusterState));

        for (final NodeRemovalClusterStateTaskExecutor.Task task : tasks) {
            assertNotNull(result.resultingState.nodes().get(task.node().getId()));
        }
    }

    private DiscoveryNode node(final int id) {
        return new DiscoveryNode(Integer.toString(id), buildNewFakeTransportAddress(), Version.CURRENT);
    }
}
