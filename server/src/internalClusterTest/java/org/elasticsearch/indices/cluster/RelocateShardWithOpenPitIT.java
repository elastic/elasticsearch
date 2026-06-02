/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.TimeUnit;

/**
 * Regression test for a {@link org.elasticsearch.index.store.Store} reference-count leak that prevents a shard from returning
 * to a node when a PIT (or any other reader context) was opened against it before it left.
 *
 * <p>A PIT's {@code ReaderContext} holds an {@code Engine.SearcherSupplier} which keeps a {@code Store.incRef} alive. When the
 * shard departs the node, the local {@code IndexShard} closes (firing {@code afterIndexRemoved} with reason
 * {@code NO_LONGER_ASSIGNED}), but {@code SearchService} deliberately keeps the reader contexts open so an open PIT continues
 * to serve its snapshot from the on-disk commit. As long as those contexts remain, the wrapper
 * {@link org.elasticsearch.env.ShardLock} for that {@code ShardId} stays held on this node.
 *
 * <p>If the shard then tries to come back to the same JVM, the new {@code IndexShard} cannot acquire the lock and recovery
 * fails with {@link org.elasticsearch.env.ShardLockObtainFailedException}. The fix is to free the stale contexts in
 * {@code SearchService.beforeIndexShardCreated}, which runs before {@code NodeEnvironment.shardLock(...)} on the relocation
 * target, so the {@code Store} ref drops and the lock releases just in time for the new {@code IndexShard} to take it.
 *
 * <p>The test pins a single-shard index to node A, opens a PIT against it, moves the shard to node B, then tries to move it
 * back to node A and asserts the return relocation completes promptly.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RelocateShardWithOpenPitIT extends ESIntegTestCase {

    public void testShardCanReturnToNodeAfterPitWasOpenedAgainstIt() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        final String index = "test-pit-lock-leak";
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.require._name", nodeA)
                .build()
        );
        ensureGreen(index);
        assertShardIsOn(index, nodeA);

        // Open a PIT against the shard on node A. The PIT creates a ReaderContext on
        // node A which holds an Engine.SearcherSupplier with a Store.incRef.
        final OpenPointInTimeResponse pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(10))
        ).actionGet();
        final BytesReference pitId = pit.getPointInTimeId();

        try {
            // Relocate the shard to node B. Node A's IndexShard closes with reason NO_LONGER_ASSIGNED; SearchService
            // deliberately keeps the PIT's ReaderContext alive on node A so the PIT can still serve its snapshot from there.
            // That kept-alive context still holds a Store.incRef, so node A's ShardLock for this ShardId stays held.
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", nodeB), index);
            ensureGreen(index);
            assertShardIsOn(index, nodeB);

            // Try to move the shard back to node A. The new IndexShard on A must acquire the same ShardLock; with the fix,
            // SearchService.beforeIndexShardCreated frees the leaked reader context on the relocation target before the lock
            // acquisition attempt, the Store ref drops to zero, the lock releases, and creation succeeds. Without the fix,
            // the lock stays held until the PIT's 10-minute keep-alive expires and the shard fails to return.
            //
            // 60s budget: with the fix the second relocation finishes in single-digit seconds; the extra margin covers slow
            // CI without making the test flaky. Without the fix we fail-fast well before any reasonable timeout.
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", nodeA), index);
            assertBusy(() -> assertShardIsOn(index, nodeA), 60, TimeUnit.SECONDS);
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    private void assertShardIsOn(String index, String expectedNodeName) {
        final var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final ShardRouting primary = state.routingTable().index(index).shard(0).primaryShard();
        assertTrue("shard not assigned: " + primary, primary.assignedToNode());
        assertTrue("shard not started: " + primary, primary.started());
        final String actualNodeName = state.nodes().get(primary.currentNodeId()).getName();
        assertEquals("shard expected on " + expectedNodeName + " but was on " + actualNodeName, expectedNodeName, actualNodeName);
    }
}
