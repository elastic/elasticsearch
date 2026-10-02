/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeIT extends ESIntegTestCase {

    private record ShardCopies(IndexShard primary, IndexShard replica) {}

    public void testForceMergeUUIDConsistent() throws IOException {
        final String index = "test-index";
        final ShardCopies shardCopies = createIndexWithPrimaryAndReplica(index);

        assertThat(getForceMergeUUID(shardCopies.primary()), nullValue());
        assertThat(getForceMergeUUID(shardCopies.replica()), nullValue());

        final BroadcastResponse forceMergeResponse = indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get();

        assertThat(forceMergeResponse.getFailedShards(), is(0));
        assertThat(forceMergeResponse.getSuccessfulShards(), is(2));

        assertForceMergeUUIDConsistentOnAllCopies(index, shardCopies);
    }

    public void testForceMergeUUIDConsistentWithOnlyExpungeDeletes() throws IOException {
        final String index = "test-index";
        final ShardCopies shardCopies = createIndexWithPrimaryAndReplica(index);

        assertThat(getForceMergeUUID(shardCopies.primary()), nullValue());
        assertThat(getForceMergeUUID(shardCopies.replica()), nullValue());

        final BroadcastResponse forceMergeResponse = indicesAdmin().prepareForceMerge(index).setOnlyExpungeDeletes(true).get();

        assertThat(forceMergeResponse.getFailedShards(), is(0));
        assertThat(forceMergeResponse.getSuccessfulShards(), is(2));

        assertForceMergeUUIDConsistentOnAllCopies(index, shardCopies);
    }

    private ShardCopies createIndexWithPrimaryAndReplica(String index) {
        internalCluster().ensureAtLeastNumDataNodes(2);
        createIndex(index, 1, 1);
        ensureGreen(index);
        final ClusterState state = clusterService().state();
        final IndexShardRoutingTable shardRouting = state.routingTable().getIndicesRouting().get(index).shard(0);
        final Index idx = shardRouting.primaryShard().index();
        return new ShardCopies(
            shardOnNode(state, shardRouting.primaryShard().currentNodeId(), idx),
            shardOnNode(state, shardRouting.replicaShards().get(0).currentNodeId(), idx)
        );
    }

    private IndexShard shardOnNode(ClusterState state, String nodeId, Index idx) {
        return internalCluster().getInstance(IndicesService.class, state.nodes().get(nodeId).getName()).indexService(idx).getShard(0);
    }

    private void assertForceMergeUUIDConsistentOnAllCopies(String index, ShardCopies copies) throws IOException {
        final String primaryForceMergeUUIDBeforeFlush = getForceMergeUUID(copies.primary());
        final String replicaForceMergeUUIDBeforeFlush = getForceMergeUUID(copies.replica());

        // Force flush to force a new commit that contains the force merge UUID
        final BroadcastResponse flushResponse = indicesAdmin().prepareFlush(index).setForce(true).get();
        assertThat(flushResponse.getFailedShards(), is(0));
        assertThat(flushResponse.getSuccessfulShards(), is(2));

        final String primaryForceMergeUUID = getForceMergeUUID(copies.primary());
        assertThat(primaryForceMergeUUID, notNullValue());
        assertThat(primaryForceMergeUUID, not(equalTo(primaryForceMergeUUIDBeforeFlush)));
        final String replicaForceMergeUUID = getForceMergeUUID(copies.replica());
        assertThat(replicaForceMergeUUID, notNullValue());
        assertThat(replicaForceMergeUUID, not(equalTo(replicaForceMergeUUIDBeforeFlush)));
        assertThat(primaryForceMergeUUID, is(replicaForceMergeUUID));
    }

    private static String getForceMergeUUID(IndexShard indexShard) throws IOException {
        try (Engine.IndexCommitRef indexCommitRef = indexShard.acquireLastIndexCommit(true)) {
            return indexCommitRef.getIndexCommit().getUserData().get(Engine.FORCE_MERGE_UUID_KEY);
        }
    }
}
