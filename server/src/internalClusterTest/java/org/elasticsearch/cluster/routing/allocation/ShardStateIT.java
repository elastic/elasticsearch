/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ShardStateIT extends ESIntegTestCase {

    public void testPrimaryFailureIncreasesTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(indexSettings(2, 1)).get();
        ensureGreen();
        assertPrimaryTerms(1, 1);

        logger.info("--> disabling allocation to capture shard failure");
        disableAllocation("test");

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        final int shard = randomBoolean() ? 0 : 1;
        final String nodeId = state.routingTable().index("test").shard(shard).primaryShard().currentNodeId();
        final String node = state.nodes().get(nodeId).getName();
        logger.info("--> failing primary of [{}] on node [{}]", shard, node);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        indicesService.indexService(resolveIndex("test")).getShard(shard).failShard("simulated test failure", null);

        logger.info("--> waiting for a yellow index");
        // we can't use ensureYellow since that one is just as happy with a GREEN status.
        assertBusy(
            () -> assertThat(client().admin().cluster().prepareHealth("test").get().getStatus(), equalTo(ClusterHealthStatus.YELLOW))
        );

        final long term0 = shard == 0 ? 2 : 1;
        final long term1 = shard == 1 ? 2 : 1;
        assertPrimaryTerms(term0, term1);

        logger.info("--> enabling allocation");
        enableAllocation("test");
        ensureGreen();
        assertPrimaryTerms(term0, term1);
    }

    protected void assertPrimaryTerms(long shard0Term, long shard1Term) {
        for (String node : internalCluster().getNodeNames()) {
            logger.debug("--> asserting primary terms terms on [{}]", node);
            ClusterState state = client(node).admin().cluster().prepareState().setLocal(true).get().getState();
            IndexMetadata metadata = state.metadata().index("test");
            assertThat(metadata.primaryTerm(0), equalTo(shard0Term));
            assertThat(metadata.primaryTerm(1), equalTo(shard1Term));
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(metadata.getIndex());
            if (indexService != null) {
                for (IndexShard shard : indexService) {
                    assertThat(
                        "term mismatch for shard " + shard.shardId(),
                        shard.getPendingPrimaryTerm(),
                        equalTo(metadata.primaryTerm(shard.shardId().id()))
                    );
                }
            }
        }
    }
}
