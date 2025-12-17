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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class TransportEnsureClusterStateVersionAppliedActionIT extends ESIntegTestCase {

    public void testVersionThatIsAlreadyApplied() {
        var masterNode = internalCluster().startMasterOnlyNode();
        var node1 = internalCluster().startNode();
        ensureStableCluster(2);

        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);
        var index = resolveIndex(indexName);

        var clusterStateVersion = new AtomicLong();
        awaitClusterState(node1, state -> {
            var projectId = state.metadata().projectFor(index).id();

            if (state.routingTable(projectId).index(index).allShardsActive()) {
                clusterStateVersion.set(state.version());
                return true;
            } else {
                return false;
            }
        });

        var future = client().admin()
            .cluster()
            .ensureClusterStateVersionApplied(
                EnsureClusterStateVersionAppliedRequest.onAllNodes(clusterStateVersion.get(), TimeValue.MINUS_ONE)
            );
        var response = future.actionGet();
        assertFalse(response.hasFailures());
        assertTrue(response.failures().isEmpty());
        assertEquals(2, response.getNodes().size());
        assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(masterNode)));
        assertTrue(response.getNodes().stream().anyMatch(r -> r.getNode().getName().equals(node1)));
    }

    public void testWaitingForVersion() throws InterruptedException {
        var masterNode = internalCluster().startMasterOnlyNode();
        var node1 = internalCluster().startNode();
        ensureStableCluster(2);

        var currentState = internalCluster().clusterService(masterNode).state();

        String masterNodeId = currentState.nodes().resolveNode(masterNode).getId();
        String node1Id = currentState.nodes().resolveNode(node1).getId();

        var onePlusVersionFuture = client().admin()
            .cluster()
            .ensureClusterStateVersionApplied(
                new EnsureClusterStateVersionAppliedRequest(currentState.version() + 1, TimeValue.MINUS_ONE, masterNodeId)
            );

        // Note that here we are waiting for two updates.
        var twoPlusVersionFuture = client().admin()
            .cluster()
            .ensureClusterStateVersionApplied(
                new EnsureClusterStateVersionAppliedRequest(currentState.version() + 2, TimeValue.MINUS_ONE, masterNodeId, node1Id)
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

    public void testNodeLevelTimeout() throws InterruptedException {
        internalCluster().startMasterOnlyNode();
        ensureStableCluster(1);

        var response = client().admin()
            .cluster()
            .ensureClusterStateVersionApplied(EnsureClusterStateVersionAppliedRequest.onAllNodes(1000000, TimeValue.timeValueMillis(100)))
            .actionGet();

        assertEquals(1, response.failures().size());
        // The structure is FailedNodeException -> RemoteTransportException -> ElasticsearchTimeoutException
        assertTrue(response.failures().get(0).getCause().getCause() instanceof ElasticsearchTimeoutException);
        assertEquals(0, response.getNodes().size());
    }

    private void dummyClusterStateUpdate(String masterNode, CountDownLatch latch) {
        internalCluster().clusterService(masterNode).submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                latch.countDown();
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

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }
}
