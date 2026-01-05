/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

@ESIntegTestCase.ClusterScope(numClientNodes = 0)
public class TransportAwaitClusterStateVersionAppliedActionIT extends ESIntegTestCase {
    public void testVersionsThatAreAlreadyApplied() {
        // Sample some of the nodes for asserts.
        internalCluster().ensureAtLeastNumDataNodes(2);
        var masterNode = internalCluster().getMasterName();
        var node1 = internalCluster().getNonMasterNodeName();

        BiConsumer<Long, Collection<DiscoveryNode>> checkAppliedVersion = (version, nodes) -> {
            var response = client().execute(
                TransportAwaitClusterStateVersionAppliedAction.TYPE,
                new AwaitClusterStateVersionAppliedRequest(version, TimeValue.MINUS_ONE, nodes.toArray(new DiscoveryNode[0]))
            ).actionGet();
            assertFalse(response.hasFailures());
            assertTrue(response.failures().isEmpty());
            assertEquals(internalCluster().numDataAndMasterNodes(), response.getNodes().size());
            assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(masterNode)));
            assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(node1)));
        };

        var initialState = internalCluster().getInstance(ClusterService.class, node1).state();
        // Succeeds because the version is applied already.
        checkAppliedVersion.accept(initialState.version(), initialState.nodes().getAllNodes());

        dummyClusterStateUpdate(masterNode, null);
        awaitClusterState(masterNode, state -> state.version() == initialState.version() + 1);

        // We should succeed again since the previous execution succeeded.
        checkAppliedVersion.accept(initialState.version(), initialState.nodes().getAllNodes());
    }

    public void testWaitingForVersion() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        var masterNode = internalCluster().getMasterName();
        var node1 = internalCluster().getNonMasterNodeName();

        var initialState = internalCluster().clusterService(masterNode).state();

        DiscoveryNode masterDiscoveryNode = initialState.nodes().resolveNode(masterNode);
        DiscoveryNode node1DiscoveryNode = initialState.nodes().resolveNode(node1);

        var onePlusVersionFuture = client().execute(
            TransportAwaitClusterStateVersionAppliedAction.TYPE,
            new AwaitClusterStateVersionAppliedRequest(initialState.version() + 1, TimeValue.MINUS_ONE, masterDiscoveryNode)
        );

        // Note that here we are waiting for two updates.
        var twoPlusVersionFuture = client().execute(
            TransportAwaitClusterStateVersionAppliedAction.TYPE,
            new AwaitClusterStateVersionAppliedRequest(
                initialState.version() + 2,
                TimeValue.MINUS_ONE,
                masterDiscoveryNode,
                node1DiscoveryNode
            )
        );

        assertFalse(onePlusVersionFuture.isDone());
        assertFalse(twoPlusVersionFuture.isDone());

        // Let's submit one cluster state update.
        var updateStarted = new CountDownLatch(2);
        dummyClusterStateUpdate(masterNode, updateStarted);

        var onePlusVersionResponse = onePlusVersionFuture.actionGet();
        // We should only get a response once the state was updated.
        assertEquals(1, updateStarted.getCount());

        assertFalse(onePlusVersionResponse.hasFailures());
        assertTrue(onePlusVersionResponse.failures().isEmpty());
        assertEquals(1, onePlusVersionResponse.getNodes().size());
        assertTrue(onePlusVersionResponse.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(masterNode)));

        // But the future waiting for two updates is still not done.
        assertFalse(twoPlusVersionFuture.isDone());

        // Submit second update.
        dummyClusterStateUpdate(masterNode, updateStarted);

        var twoPlusVersionResponse = twoPlusVersionFuture.actionGet();
        assertEquals(0, updateStarted.getCount());

        assertFalse(twoPlusVersionResponse.hasFailures());
        assertTrue(twoPlusVersionResponse.failures().isEmpty());
        assertEquals(2, twoPlusVersionResponse.getNodes().size());
        assertTrue(twoPlusVersionResponse.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(masterNode)));
        assertTrue(twoPlusVersionResponse.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(node1)));
    }

    public void testNodeNotProcessingClusterState() throws InterruptedException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        var masterNode = internalCluster().getMasterName();
        var node1 = internalCluster().getNonMasterNodeName();

        var initialState = internalCluster().clusterService(masterNode).state();

        DiscoveryNode masterDiscoveryNode = initialState.nodes().resolveNode(masterNode);
        DiscoveryNode node1DiscoveryNode = initialState.nodes().resolveNode(node1);

        // Wait for the future version of the cluster state.
        var future = client().execute(
            TransportAwaitClusterStateVersionAppliedAction.TYPE,
            new AwaitClusterStateVersionAppliedRequest(
                initialState.version() + 1,
                TimeValue.MINUS_ONE,
                masterDiscoveryNode,
                node1DiscoveryNode
            )
        );

        assertFalse(future.isDone());

        // Now we'll block node1 from processing cluster state updates.
        var clusterStatePublishLatch = new CountDownLatch(1);
        final var node1TransportService = MockTransportService.getInstance(node1);
        node1TransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (clusterStatePublishLatch.getCount() > 0) {
                    clusterStatePublishLatch.await();
                }
                handler.messageReceived(request, channel, task);
            }
        );

        // And publish a new state.
        var publishLatch = new CountDownLatch(1);
        dummyClusterStateUpdate(masterNode, publishLatch);
        publishLatch.await();

        try {
            // We don't get a response since we are waiting for node1.
            assertThrows(ElasticsearchTimeoutException.class, () -> future.actionGet(TimeValue.timeValueMillis(500)));
        } finally {
            clusterStatePublishLatch.countDown();
        }

        // Once node1 gets the new cluster state we get a response.
        var response = future.actionGet();
        assertFalse(response.hasFailures());
        assertTrue(response.failures().isEmpty());
        assertEquals(2, response.getNodes().size());
        assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(masterNode)));
        assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(node1)));
    }

    public void testTimeout() {
        var currentState = internalCluster().getInstance(ClusterService.class).state();

        var response = client().execute(
            TransportAwaitClusterStateVersionAppliedAction.TYPE,
            new AwaitClusterStateVersionAppliedRequest(
                currentState.version() + 100,
                TimeValue.timeValueMillis(100),
                currentState.nodes().getAllNodes().toArray(new DiscoveryNode[0])
            )
        ).actionGet();

        assertEquals(internalCluster().numDataAndMasterNodes(), response.failures().size());
        // The structure is FailedNodeException -> RemoteTransportException -> ElasticsearchTimeoutException
        assertTrue(response.failures().get(0).getCause().getCause() instanceof ElasticsearchTimeoutException);
        assertEquals(0, response.getNodes().size());
    }

    public void testCancellation() {
        var currentState = internalCluster().getInstance(ClusterService.class).state();

        var future = client().execute(
            TransportAwaitClusterStateVersionAppliedAction.TYPE,
            new AwaitClusterStateVersionAppliedRequest(
                currentState.version() + 100,
                TimeValue.MINUS_ONE,
                currentState.nodes().getAllNodes().toArray(new DiscoveryNode[0])
            )
        );

        var tasks = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransportAwaitClusterStateVersionAppliedAction.TYPE.name())
            .get()
            .getTasks();
        assertEquals(1, tasks.size());
        var thisTask = tasks.get(0);

        assertFalse(future.isDone());

        var cancelRequest = new CancelTasksRequest().setTargetTaskId(thisTask.taskId()).setReason("cancelled");
        client().execute(TransportCancelTasksAction.TYPE, cancelRequest);

        assertThrows(TaskCancelledException.class, () -> future.actionGet(SAFE_AWAIT_TIMEOUT));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    private void dummyClusterStateUpdate(String masterNode, CountDownLatch latch) {
        internalCluster().clusterService(masterNode).submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (latch != null) {
                    latch.countDown();
                }
                return ClusterState.builder(currentState)
                    .metadata(
                        currentState.metadata()
                            .withAddedIndex(
                                IndexMetadata.builder(randomIdentifier())
                                    .settings(indexSettings(IndexVersion.current(), randomIdentifier(), 1, 0))
                                    .build()
                            )
                    )
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
    }
}
