/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for {@code SAMPLE} that need more than one shard.
 */
// One data node, so that all the shards of an index are planned together.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SampleIT extends AbstractEsqlIntegTestCase {

    /**
     * A random {@code max_concurrent_shards_per_node} splits the shards into batches that each sample with a seed of
     * their own, which hides shards that share one.
     */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Shards that share a random stream keep the same doc ids: with one document each, all of them or none.
     */
    public void testShardsSampleDifferentDocs() {
        String index = "sample_one_doc_per_shard";
        int shards = 10;
        assertThat("the shards have to fit in a single batch", shards, lessThanOrEqualTo(getPragmas().maxConcurrentShardsPerNode()));
        assertAcked(prepareCreate(index).setSettings(indexSettings(shards, 0)).setMapping("shard", "type=integer"));
        for (int shard = 0; shard < shards; shard++) {
            prepareIndex(index).setRouting(routingKeyForShard(index, shard)).setSource("shard", shard).get();
        }
        refresh(index);
        for (ShardStats shardStats : indicesAdmin().prepareStats(index).clear().setDocs(true).get().getShards()) {
            assertThat(shardStats.getShardRouting().toString(), shardStats.getStats().getDocs().getCount(), equalTo(1L));
        }

        // Each query is all-or-none with probability 1/512, so 20 in a row is practically impossible.
        boolean sampledSomeOfTheShards = false;
        for (int i = 0; i < 20 && sampledSomeOfTheShards == false; i++) {
            try (EsqlQueryResponse response = run("FROM " + index + " | SAMPLE 0.5 | STATS c = COUNT(*)")) {
                long count = as(singleValue(singleValue(getValuesList(response))), Long.class);
                sampledSomeOfTheShards = 0 < count && count < shards;
            }
        }
        assertTrue("every query sampled either all of the shards or none of them", sampledSomeOfTheShards);
    }
}
