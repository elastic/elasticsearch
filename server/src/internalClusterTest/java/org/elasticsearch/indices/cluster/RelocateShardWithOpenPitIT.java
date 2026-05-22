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
 * Regression test for a Store reference-count leak that prevents a shard from returning
 * to a node when a PIT (or any reader context) was opened against it before it relocated
 * away.
 *
 * <p>{@code SearchService.afterIndexRemoved} frees reader contexts on
 * {@code DELETED/CLOSED/REOPENED} but not on {@code NO_LONGER_ASSIGNED} - which is the
 * reason fired when a shard relocates off a node. The PIT's
 * {@code Engine.SearcherSupplier} holds a {@code Store.incRef}, so without that cleanup
 * the wrapper {@code ShardLock} stays held on the old node and any attempt to allocate
 * the same {@code ShardId} back to that node fails with
 * {@code ShardLockObtainFailedException}.
 *
 * <p>The test pins a single-shard index to node A, opens a PIT, moves the shard to
 * node B, then tries to move it back to node A and asserts the move completes.
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
            // Relocate the shard to node B. Node A's IndexShard closes with reason
            // NO_LONGER_ASSIGNED. Without the fix, SearchService.afterIndexRemoved does
            // not fire for that reason, the PIT keeps its Store ref, and node A's
            // ShardLock stays held.
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", nodeB), index);
            ensureGreen(index);
            assertShardIsOn(index, nodeB);

            // Try to move the shard back to node A. The new IndexShard needs to
            // acquire node A's ShardLock for the same ShardId; without the fix it
            // can't because the old (leaked) ref is still holding it.
            // 60s budget: with the fix the second relocation finishes in single-digit
            // seconds; the extra margin covers slow CI without making the test flaky.
            // Without the fix the shard never returns (lock held until PIT expires at
            // 10 min), so we fail-fast well before any reasonable timeout.
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
