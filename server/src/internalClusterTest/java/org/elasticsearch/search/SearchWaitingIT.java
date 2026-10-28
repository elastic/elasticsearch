/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A search waiting on a data node whose search threads are all busy is tracked as one query, however many of its shard
 * tasks are queued, and reports its search waiting on the shards it searched.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchWaitingIT extends ESIntegTestCase {

    private static final int SEARCH_THREADS = 2;

    public void testBatchedQueryPhase() throws Exception {
        assertWaitingIsTrackedPerQuery(true);
    }

    public void testShardQueryPhase() throws Exception {
        assertWaitingIsTrackedPerQuery(false);
    }

    private void assertWaitingIsTrackedPerQuery(boolean batched) throws Exception {
        String dataNode = internalCluster().startNode(Settings.builder().put("thread_pool.search.size", SEARCH_THREADS).build());
        String coordinator = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), batched).build()
        );
        int shards = between(SEARCH_THREADS + 1, 8);
        createIndex("test", indexSettings(shards, 0).build());
        createIndex("other", indexSettings(1, 0).build());
        int docs = between(1, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("test").setSource("field", i).get();
        }
        refresh("test");

        SearchService searchService = internalCluster().getInstance(SearchService.class, dataNode);
        Executor searchPool = internalCluster().getInstance(ThreadPool.class, dataNode).executor(ThreadPool.Names.SEARCH);
        CountDownLatch blocked = new CountDownLatch(SEARCH_THREADS);
        CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < SEARCH_THREADS; i++) {
            searchPool.execute(() -> {
                blocked.countDown();
                safeAwait(release);
            });
        }
        safeAwait(blocked);

        ActionFuture<SearchResponse> future = client(coordinator).prepareSearch("test").setSize(docs).execute();
        try {
            // all shard tasks of the search are queued behind the blocked threads, as one query
            assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(1)));
            // the waiting shows while the tasks are still queued
            assertBusy(() -> assertThat(searchWaiting("test").getSearchWaitingRate(), greaterThan(0.0)));
        } finally {
            release.countDown();
        }
        assertResponse(future, response -> assertHitCount(response, docs));
        assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(0)));

        assertThat(searchWaiting("test").getSearchWaitingRate(), greaterThan(0.0));
        assertThat(searchWaiting("other").getSearchWaitingRate(), equalTo(0.0));
    }

    private static SearchStats.Stats searchWaiting(String index) {
        return indicesAdmin().prepareStats(index).setSearch(true).get().getIndex(index).getTotal().getSearch().getTotal();
    }
}
