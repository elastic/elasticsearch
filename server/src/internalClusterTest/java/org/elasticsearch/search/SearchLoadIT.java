/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The time of the tasks a search runs on a data node, its shard phases, the segment searches they fork and the partial
 * reduces of their results, is the search load of the shards it searches.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchLoadIT extends ESIntegTestCase {

    public void testShardTasksChargeTheShardsTheySearch() throws Exception {
        String dataNode = internalCluster().startNode(
            Settings.builder()
                // no reaper of search contexts on the search pool while the searches run
                .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
                // segments searched in parallel, however small: with 4 threads a slice needs a quarter of the shard's documents,
                // less than each of its 3 segments holds
                .put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 1)
                .put("thread_pool.search.size", 4)
        );
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        // more shards on the data node than the batched reduce size, so it reduces their results in a task of its own
        createIndex("searched", indexSettings(between(3, 5), 0).build());
        createIndex("other", indexSettings(1, 0).build());
        // a refresh after each round, so each shard has several segments to search in parallel
        for (int segment = 0; segment < 3; segment++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < 500; i++) {
                bulk.add(prepareIndex(randomFrom("searched", "other")).setSource("k", "k" + (i % 10), "v", i));
            }
            assertNoFailures(bulk.get());
        }
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, dataNode);
        var search = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        long attributedBefore = search.getAttributedTaskExecutionTime();
        long totalBefore = search.getTotalTaskExecutionTime();
        long chargedBefore = chargedToShards(dataNode);

        for (int i = between(1, 5); i > 0; i--) {
            // sorted by score, since sorting on a field searches segments one after the other
            var request = client(coordinator).prepareSearch("searched")
                .setQuery(QueryBuilders.rangeQuery("v").gte(between(0, 50)))
                .setSize(10)
                .setBatchedReduceSize(2);
            if (randomBoolean()) {
                request.addAggregation(AggregationBuilders.terms("k").field("k.keyword"));
            }
            var response = request.get();
            try {
                assertNoFailures(response);
            } finally {
                response.decRef();
            }
        }

        assertBusy(() -> {
            long total = search.getTotalTaskExecutionTime() - totalBefore;
            assertThat(total, greaterThan(0L));
            // every task is charged, and charged once
            assertThat(search.getAttributedTaskExecutionTime() - attributedBefore, equalTo(total));
            assertThat(chargedToShards(dataNode) - chargedBefore, equalTo(total));
        });
        assertThat(searchLoad("searched"), greaterThan(0.0));
        assertThat(searchLoad("other"), equalTo(0.0));
    }

    private static long chargedToShards(String node) {
        long charged = 0;
        for (IndexService indexService : internalCluster().getInstance(IndicesService.class, node)) {
            for (IndexShard shard : indexService) {
                charged += shard.shardSearchStats().totalSearchLoadNanos();
            }
        }
        return charged;
    }

    private static double searchLoad(String index) {
        return indicesAdmin().prepareStats(index)
            .setSearch(true)
            .get()
            .getIndex(index)
            .getTotal()
            .getSearch()
            .getTotal()
            .getSearchLoadRate();
    }
}
