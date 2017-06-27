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

package org.elasticsearch.index.shard;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that a primary shard tracks its own local checkpoint after starting.
 */
@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class LocalCheckpointIT extends ESIntegTestCase {

    public void testGatewayRecovery() throws Exception {
        internalCluster().startNode();

        assertAcked(
                prepareCreate(
                        "index",
                        Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        ensureGreen();

        final int numDocs = scaledRandomIntBetween(0, 128);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("index", "type").setSource("f", randomInt());
        }

        indexRandom(false, docs);

        internalCluster().fullRestart();
        ensureGreen();

        final IndexShard indexShard = getIndexShard();
        assertLocalCheckpoint(numDocs, indexShard);
    }

    public void testPrimaryPromotion() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();

        assertAcked(prepareCreate("index", Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 1)));
        ensureGreen();

        final int numDocs = scaledRandomIntBetween(0, 128);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("index", "type").setSource("f", randomInt());
        }

        indexRandom(false, docs);

        final IndexShard primaryShardBeforeFailure = getIndexShard();

        primaryShardBeforeFailure.failShard("test", new Exception("test"));
        // this can not succeed until the primary shard has failed and the replica shard is promoted
        client().admin().indices().prepareRefresh("index").get();
        ensureGreen();

        final IndexShard primaryShardAfterFailure = getIndexShard();

        assertLocalCheckpoint(numDocs, primaryShardAfterFailure);
    }

    public void testPrimaryRelocation() throws ExecutionException, InterruptedException {
        final String sourceNode = internalCluster().startNode();

        assertAcked(prepareCreate("index", Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        ensureGreen();

        final int numDocs = scaledRandomIntBetween(0, 128);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("index", "type").setSource("f", randomInt());
        }

        indexRandom(false, docs);

        final String targetNode = internalCluster().startNode();

        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("index", 0, sourceNode, targetNode)).get();
        final ClusterHealthResponse response =
                client()
                        .admin()
                        .cluster()
                        .prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForNoRelocatingShards(true)
                        .setTimeout(TimeValue.timeValueMinutes(1)).get();
        assertFalse(response.isTimedOut());

        final IndexShard indexShard = getIndexShard();
        assertLocalCheckpoint(numDocs, indexShard);
    }

    private IndexShard getIndexShard() {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final String nodeId = state.routingTable().index("index").shard(0).primaryShard().currentNodeId();
        final String node = state.nodes().get(nodeId).getName();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        return indicesService.indexService(resolveIndex("index")).getShard(0);
    }

    private void assertLocalCheckpoint(final int numDocs, final IndexShard indexShard) {
        assertThat(indexShard.getLocalCheckpoint(), equalTo((long)(numDocs - 1)));
        assertThat(
                indexShard.getEngine().seqNoService().getLocalCheckpointForInSyncShard(indexShard.routingEntry().allocationId().getId()),
                equalTo(indexShard.getLocalCheckpoint()));
    }

}
