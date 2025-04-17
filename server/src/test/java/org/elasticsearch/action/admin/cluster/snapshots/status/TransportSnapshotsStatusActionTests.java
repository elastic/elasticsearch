/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TransportSnapshotsStatusActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private RepositoriesService repositoriesService;
    private TransportSnapshotsStatusAction action;

    @Before
    public void initializeComponents() throws Exception {
        threadPool = new TestThreadPool(TransportSnapshotsStatusActionTests.class.getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        transportService = new CapturingTransport().createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        final var nodeClient = new NodeClient(clusterService.getSettings(), threadPool);
        repositoriesService = new RepositoriesService(
            clusterService.getSettings(),
            clusterService,
            Map.of(),
            Map.of(),
            threadPool,
            nodeClient,
            List.of()
        );
        action = new TransportSnapshotsStatusAction(
            transportService,
            clusterService,
            threadPool,
            repositoriesService,
            nodeClient,
            new ActionFilters(Set.of())
        );
    }

    @After
    public void shutdownComponents() throws Exception {
        threadPool.shutdown();
        repositoriesService.close();
        transportService.close();
        clusterService.close();
    }

    public void testBuildResponseDetectsTaskIsCancelledWhileProcessingCurrentSnapshotEntries() throws Exception {
        runBasicBuildResponseTest(true);
    }

    public void testBuildResponseInvokesListenerWithResponseWhenTaskIsNotCancelled() throws Exception {
        runBasicBuildResponseTest(false);
    }

    private void runBasicBuildResponseTest(boolean shouldCancelTask) {
        final var expectedSnapshot = new Snapshot(ProjectId.DEFAULT, "test-repo", new SnapshotId("snapshot", "uuid"));
        final var expectedState = SnapshotsInProgress.State.STARTED;
        final var indexName = "test-index-name";
        final var indexUuid = "test-index-uuid";
        final var currentSnapshotEntries = List.of(
            SnapshotsInProgress.Entry.snapshot(
                expectedSnapshot,
                randomBoolean(),
                randomBoolean(),
                SnapshotsInProgress.State.STARTED,
                Map.of(indexName, new IndexId(indexName, indexUuid)),
                List.of(),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(
                    new ShardId(indexName, indexUuid, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus("node", new ShardGeneration("gen"))
                ),
                null,
                Map.of(),
                IndexVersion.current()
            )
        );
        final var nodeSnapshotStatuses = new TransportNodesSnapshotsStatus.NodesSnapshotStatus(
            clusterService.getClusterName(),
            List.of(),
            List.of()
        );

        // Run some sanity checks for when the task is not cancelled and we get back a response object.
        // Note that thorough verification of the SnapshotsStatusResponse is done in the higher level SnapshotStatus API integration tests.
        final Consumer<SnapshotsStatusResponse> verifyResponse = rsp -> {
            assertNotNull(rsp);
            final var snapshotStatuses = rsp.getSnapshots();
            assertNotNull(snapshotStatuses);
            assertEquals(
                "expected 1 snapshot status, got " + snapshotStatuses.size() + ": " + snapshotStatuses,
                1,
                snapshotStatuses.size()
            );
            final var snapshotStatus = snapshotStatuses.getFirst();
            assertNotNull(snapshotStatus.getSnapshot());
            assertEquals(expectedSnapshot, snapshotStatus.getSnapshot());
            assertEquals(expectedState, snapshotStatus.getState());
            final var snapshotStatusShards = snapshotStatus.getShards();
            assertNotNull(snapshotStatusShards);
            assertEquals(
                "expected 1 index shard status, got " + snapshotStatusShards.size() + ": " + snapshotStatusShards,
                1,
                snapshotStatusShards.size()
            );
            final var snapshotStatusIndices = snapshotStatus.getIndices();
            assertNotNull(snapshotStatusIndices);
            assertEquals(
                "expected 1 entry in snapshotStatusIndices, got " + snapshotStatusIndices.size() + ": " + snapshotStatusIndices,
                1,
                snapshotStatusIndices.size()
            );
            assertTrue(
                "no entry for indexName [" + indexName + "] found in snapshotStatusIndices keyset " + snapshotStatusIndices.keySet(),
                snapshotStatusIndices.containsKey(indexName)
            );
            assertNotNull(snapshotStatus.getShardsStats());
        };

        final var listener = new ActionListener<SnapshotsStatusResponse>() {
            @Override
            public void onResponse(SnapshotsStatusResponse rsp) {
                if (shouldCancelTask) {
                    fail("expected detection of task cancellation and onFailure() instead of onResponse(" + rsp + ")");
                } else {
                    verifyResponse.accept(rsp);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (shouldCancelTask) {
                    assertTrue(e instanceof TaskCancelledException);
                } else {
                    fail("expected onResponse() instead of onFailure(" + e + ")");
                }
            }
        };

        final var listenerInvoked = new AtomicBoolean(false);
        final var cancellableTask = new CancellableTask(randomLong(), "type", "action", "desc", null, Map.of());

        if (shouldCancelTask) {
            TaskCancelHelper.cancel(cancellableTask, "simulated cancellation");
        }

        action.buildResponse(
            SnapshotsInProgress.EMPTY,
            new SnapshotsStatusRequest(TEST_REQUEST_TIMEOUT),
            currentSnapshotEntries,
            nodeSnapshotStatuses,
            cancellableTask,
            ActionListener.runAfter(listener, () -> listenerInvoked.set(true))
        );
        assertTrue("Expected listener to be invoked", listenerInvoked.get());
    }
}
