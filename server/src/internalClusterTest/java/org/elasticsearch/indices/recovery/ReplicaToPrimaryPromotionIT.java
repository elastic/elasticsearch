/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class ReplicaToPrimaryPromotionIT extends ESIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testPromoteReplicaToPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        if (numOfDocs > 0) {
            try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, client(), numOfDocs)) {
                waitForDocs(numOfDocs, indexer);
            }
            refresh(indexName);
        }

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
        ensureGreen(indexName);

        // sometimes test with a closed index
        final IndexMetadata.State indexState = randomFrom(IndexMetadata.State.OPEN, IndexMetadata.State.CLOSE);
        if (indexState == IndexMetadata.State.CLOSE) {
            CloseIndexResponse closeIndexResponse = indicesAdmin().prepareClose(indexName).get();
            assertThat("close index not acked - " + closeIndexResponse, closeIndexResponse.isAcknowledged(), equalTo(true));
            ensureGreen(indexName);
        }

        // pick up a data node that contains a random primary shard
        ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        final int numShards = state.metadata().index(indexName).getNumberOfShards();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(randomIntBetween(0, numShards - 1)).primaryShard();
        final DiscoveryNode randomNode = state.nodes().resolveNode(primaryShard.currentNodeId());

        // stop the random data node, all remaining shards are promoted to primaries
        internalCluster().stopNode(randomNode.getName());
        ensureYellowAndNoInitializingShards(indexName);

        state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        final IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            for (ShardRouting shardRouting : indexRoutingTable.shard(i).activeShards()) {
                assertThat(shardRouting + " should be promoted as a primary", shardRouting.primary(), is(true));
            }
        }

        if (indexState == IndexMetadata.State.CLOSE) {
            assertAcked(indicesAdmin().prepareOpen(indexName));
            ensureYellowAndNoInitializingShards(indexName);
        }
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
    }
}
