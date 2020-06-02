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

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeIT extends ESIntegTestCase {

    public void testForceMergeUUIDConsistent() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String index = "test-index";
        createIndex(index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureGreen(index);
        final ClusterState state = clusterService().state();
        final IndexRoutingTable indexShardRoutingTables = state.routingTable().getIndicesRouting().get(index);
        final IndexShardRoutingTable shardRouting = indexShardRoutingTables.getShards().get(0);
        final String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        final String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();
        final Index idx = shardRouting.primaryShard().index();
        final IndicesService primaryIndicesService =
            internalCluster().getInstance(IndicesService.class, state.nodes().get(primaryNodeId).getName());
        final IndicesService replicaIndicesService = internalCluster().getInstance(IndicesService.class,
            state.nodes().get(replicaNodeId).getName());
        final IndexShard primary = primaryIndicesService.indexService(idx).getShard(0);
        final IndexShard replica = replicaIndicesService.indexService(idx).getShard(0);

        assertThat(getForceMergeUUID(primary), nullValue());
        assertThat(getForceMergeUUID(replica), nullValue());

        final ForceMergeResponse forceMergeResponse =
            client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        assertThat(forceMergeResponse.getFailedShards(), is(0));
        assertThat(forceMergeResponse.getSuccessfulShards(), is(2));

        // Force flush to force a new commit that contains the force flush UUID
        final FlushResponse flushResponse = client().admin().indices().prepareFlush(index).setForce(true).get();
        assertThat(flushResponse.getFailedShards(), is(0));
        assertThat(flushResponse.getSuccessfulShards(), is(2));

        final String primaryForceMergeUUID = getForceMergeUUID(primary);
        assertThat(primaryForceMergeUUID, notNullValue());

        final String replicaForceMergeUUID = getForceMergeUUID(replica);
        assertThat(replicaForceMergeUUID, notNullValue());
        assertThat(primaryForceMergeUUID, is(replicaForceMergeUUID));
    }

    private static String getForceMergeUUID(IndexShard indexShard) throws IOException {
        try (Engine.IndexCommitRef indexCommitRef = indexShard.acquireLastIndexCommit(true)) {
            return indexCommitRef.getIndexCommit().getUserData().get(Engine.FORCE_MERGE_UUID_KEY);
        }
    }
}
