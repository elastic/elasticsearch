/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.RestoreService.RestoreCompletionResponse;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link RestoreLifecycleListener} covering the two callbacks
 * ({@code onRestoreInitialized} and {@code onRestoreCompleted}) and the idempotency guarantee
 * that {@code onRestoreInitialized} is not called twice when the same restore UUID is submitted
 * while the first restore is still in progress.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RestoreLifecycleListenerIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String SNAP = "test-snap";
    private static final String IDX = "test-idx";

    /**
     * Verifies that {@code onRestoreInitialized} fires exactly once when the cluster-state update
     * installs the {@link RestoreInProgress} entry, and that {@code onRestoreCompleted} fires
     * exactly once when the cleanup task removes it.
     */
    public void testLifecycleCallbacksFireOnceForOrdinaryRestore() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        createRepository(REPO, "mock");
        createIndex(IDX, 1, 0);
        indexRandomDocs(IDX, between(1, 50));
        ensureGreen(IDX);
        createFullSnapshot(REPO, SNAP);
        cluster().wipeIndices(IDX);

        AtomicInteger initCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);

        internalCluster().getInstance(RestoreService.class, internalCluster().getMasterName())
            .setLifecycleListener(new RestoreLifecycleListener() {
                @Override
                public ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
                    initCount.incrementAndGet();
                    return state;
                }

                @Override
                public ClusterState onRestoreCompleted(RestoreInProgress.Entry entry, ClusterState state) {
                    completedCount.incrementAndGet();
                    return state;
                }
            });

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO, SNAP).setIndices(IDX).setWaitForCompletion(true).get();

        assertThat(initCount.get(), equalTo(1));
        assertThat(completedCount.get(), equalTo(1));
    }

    /**
     * Verifies that when a second call arrives with the same restore UUID while the first restore is
     * still in progress, {@code execute()} returns the cluster state unchanged and
     * {@code onRestoreInitialized} is NOT called a second time.
     *
     * <p>The restore is kept in-flight by blocking the mock repository on all data nodes.
     */
    public void testOnRestoreInitializedNotCalledTwiceForSameUUID() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode(SMALL_SNAPSHOT_POOL_SETTINGS);

        createRepository(REPO, "mock");
        createIndex(IDX, 1, 0);
        indexRandomDocs(IDX, between(1, 50));
        ensureGreen(IDX);
        createFullSnapshot(REPO, SNAP);
        cluster().wipeIndices(IDX);

        AtomicInteger initCount = new AtomicInteger(0);

        String masterName = internalCluster().getMasterName();
        RestoreService restoreService = internalCluster().getInstance(RestoreService.class, masterName);
        restoreService.setLifecycleListener(new RestoreLifecycleListener() {
            @Override
            public ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
                initCount.incrementAndGet();
                return state;
            }
        });

        // Block the data node so shard recovery stalls and the RestoreInProgress entry stays in cluster state.
        blockAllDataNodes(REPO);

        String restoreUUID = UUIDs.randomBase64UUID();
        var request = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO, SNAP)
            .setIndices(IDX)
            .setWaitForCompletion(false)
            .request();

        // First restore call — async, will not complete until data node is unblocked.
        PlainActionFuture<RestoreCompletionResponse> firstFuture = new PlainActionFuture<>();
        restoreService.restoreSnapshot(ProjectId.DEFAULT, request, restoreUUID, firstFuture, (s, b) -> {});

        // Wait until the data node is actually stuck in the repo block (shard recovery started).
        waitForBlock(dataNode, REPO);
        assertThat("onRestoreInitialized must have fired exactly once after first call", initCount.get(), equalTo(1));
        // Confirm the entry is visible in cluster state.
        assertNotNull(
            "RestoreInProgress entry for UUID [" + restoreUUID + "] must be present",
            RestoreInProgress.get(internalCluster().getInstance(ClusterService.class, masterName).state()).get(restoreUUID)
        );

        // Second call with the same UUID — idempotent retry; onRestoreInitialized must NOT fire again.
        PlainActionFuture<RestoreCompletionResponse> secondFuture = new PlainActionFuture<>();
        restoreService.restoreSnapshot(ProjectId.DEFAULT, request, restoreUUID, secondFuture, (s, b) -> {});

        // The second call's cluster-state task returns early (UUID already present) and resolves
        // with a null restoreInfo, signalling the caller that nothing was applied.
        RestoreCompletionResponse secondResponse = secondFuture.actionGet(10, TimeUnit.SECONDS);
        assertThat("idempotent retry must return null restoreInfo", secondResponse.restoreInfo(), nullValue());
        assertThat("onRestoreInitialized must still have fired only once", initCount.get(), equalTo(1));

        // Unblock the data node and let the first restore finish normally.
        unblockAllDataNodes(REPO);
        assertBusy(
            () -> assertTrue(
                "RestoreInProgress must be empty once restore completes",
                RestoreInProgress.get(internalCluster().getInstance(ClusterService.class, masterName).state()).isEmpty()
            )
        );
    }
}
