/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class TransportClusterStateActionDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNonLocalRequestAlwaysFindsMaster() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final ClusterStateRequestBuilder clusterStateRequestBuilder = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setNodes(true)
                .setBlocks(true)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100));
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            assertNotNull("should always contain a master node", clusterStateResponse.getState().nodes().getMasterNodeId());
        });
    }

    public void testLocalRequestAlwaysSucceeds() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final DiscoveryNodes discoveryNodes = client(node).admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setLocal(true)
                .setNodes(true)
                .setBlocks(true)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100))
                .get()
                .getState()
                .nodes();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.getName().equals(node)) {
                    return;
                }
            }
            fail("nodes did not contain [" + node + "]: " + discoveryNodes);
        });
    }

    public void testNonLocalRequestAlwaysFindsMasterAndWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion = internalCluster().getInstance(ClusterService.class, node)
                .getClusterApplierService()
                .state()
                .metadata()
                .version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateRequestBuilder clusterStateRequestBuilder = client(node).admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setNodes(true)
                .setMetadata(true)
                .setBlocks(true)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100))
                .setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .setWaitForMetadataVersion(waitForMetadataVersion);
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final ClusterState state = clusterStateResponse.getState();
                assertNotNull("should always contain a master node", state.nodes().getMasterNodeId());
                assertThat("waited for metadata version", state.metadata().version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void testLocalRequestWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion = internalCluster().getInstance(ClusterService.class, node)
                .getClusterApplierService()
                .state()
                .metadata()
                .version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateResponse clusterStateResponse = client(node).admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setLocal(true)
                .setMetadata(true)
                .setBlocks(true)
                .setWaitForMetadataVersion(waitForMetadataVersion)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100))
                .setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .get();
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final Metadata metadata = clusterStateResponse.getState().metadata();
                assertThat(
                    "waited for metadata version " + waitForMetadataVersion + " with node " + node,
                    metadata.version(),
                    greaterThanOrEqualTo(waitForMetadataVersion)
                );
            }
        });
    }

    public void runRepeatedlyWhileChangingMaster(Runnable runnable) throws Exception {
        internalCluster().startNodes(3);

        ClusterServiceUtils.awaitClusterState(
            cs -> cs.getLastCommittedConfiguration()
                .getNodeIds()
                .stream()
                .filter(Predicate.not(ClusterBootstrapService::isBootstrapPlaceholder))
                .collect(Collectors.toSet())
                .size() == 3,
            internalCluster().getInstance(ClusterService.class)
        );

        final String masterName = internalCluster().getMasterName();

        final AtomicBoolean shutdown = new AtomicBoolean();
        final Thread assertingThread = new Thread(() -> {
            while (shutdown.get() == false) {
                runnable.run();
            }
        }, "asserting thread");
        assertingThread.start();

        final var oldMasterUpdatesCompletedLatch = new CountDownLatch(between(1, 5));
        final var updatesCompleteLatch = new CountDownLatch(1);
        final var newMasterElectedLatch = new CountDownLatch(1);
        final var newMasterUpdatesCompletedLatch = new CountDownLatch(between(1, 5));
        try (
            var updatesRunningRefs = new RefCountingRunnable(updatesCompleteLatch::countDown);
            var awaitingNewMasterRefs = new RefCountingRunnable(newMasterElectedLatch::countDown)
        ) {
            for (var clusterService : internalCluster().getInstances(ClusterService.class)) {
                class UpdateTask {

                    private final Releasable awaitingNewMasterRef = awaitingNewMasterRefs.acquire();
                    private final Releasable updatesRunningRef = updatesRunningRefs.acquire();
                    private final boolean isOriginalMaster = clusterService.localNode().getName().equals(masterName);

                    public void start() {
                        ClusterServiceUtils.addTemporaryStateListener(
                            clusterService,
                            isOriginalMaster
                                ? state -> state.nodes().getMasterNode() == null
                                : state -> Optional.ofNullable(state.nodes().getMasterNode())
                                    .map(n -> masterName.equals(n.getName()) == false)
                                    .orElse(false)
                                    && state.nodes().stream().noneMatch(n -> n.getName().equals(masterName))
                        ).addListener(ActionTestUtils.assertNoFailureListener(v -> awaitingNewMasterRef.close()));

                        submitLoopingUpdateTask();
                    }

                    public void submitLoopingUpdateTask() {
                        if (shutdown.get()) {
                            updatesRunningRef.close();
                            return;
                        }

                        clusterService.submitUnbatchedStateUpdateTask("test update", new ClusterStateUpdateTask() {
                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                // perform a no-op update of the Metadata, forcing a new version and triggering a publication
                                return ClusterState.builder(currentState)
                                    .metadata(Metadata.builder(currentState.metadata()).build())
                                    .build();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (MasterService.isPublishFailureException(e)) {
                                    submitLoopingUpdateTask();
                                } else {
                                    fail(e);
                                }
                            }

                            @Override
                            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                                assertThat(newState.metadata().version(), greaterThan(initialState.metadata().version()));
                                (isOriginalMaster ? oldMasterUpdatesCompletedLatch : newMasterUpdatesCompletedLatch).countDown();
                                submitLoopingUpdateTask();
                            }
                        });
                    }
                }

                new UpdateTask().start();
            }
        }

        final List<MockTransportService> mockTransportServices = StreamSupport.stream(
            internalCluster().getInstances(TransportService.class).spliterator(),
            false
        ).map(ts -> asInstanceOf(MockTransportService.class, ts)).toList();

        final var masterTransportService = MockTransportService.getInstance(masterName);

        safeAwait(oldMasterUpdatesCompletedLatch);

        for (final var mockTransportService : mockTransportServices) {
            if (masterTransportService != mockTransportService) {
                masterTransportService.addFailToSendNoConnectRule(mockTransportService);
                mockTransportService.addFailToSendNoConnectRule(masterTransportService);
            }
        }

        safeAwait(newMasterElectedLatch);
        safeAwait(newMasterUpdatesCompletedLatch);
        shutdown.set(true);
        assertingThread.join();
        safeAwait(updatesCompleteLatch);
        internalCluster().close();
    }

    public void testFailsWithBlockExceptionIfBlockedAndBlocksNotRequested() {
        internalCluster().startMasterOnlyNode(Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 1).build());
        final var state = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).execute()).getState();
        assertTrue(state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        assertThat(
            safeAwaitFailure(
                SubscribableListener.<ClusterStateResponse>newForked(
                    l -> clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().execute(l)
                )
            ),
            instanceOf(ClusterBlockException.class)
        );

        internalCluster().startDataOnlyNode();

        final var recoveredState = safeGet(clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(randomBoolean()).execute())
            .getState();
        assertFalse(recoveredState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
    }

}
