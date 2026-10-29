/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * An ES|QL query waiting on a data node whose worker threads are all busy is tracked as one query, however many of its
 * drivers are queued, and reports its search waiting on the shards it searched.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlSearchWaitingIT extends AbstractEsqlIntegTestCase {

    private static final int WORKER_THREADS = 2;

    public void testDriversWaitAsOneQuery() throws Exception {
        String dataNode = internalCluster().startNode(
            Settings.builder().put(EsqlPlugin.ESQL_WORKER_THREAD_POOL_SIZE.getKey(), WORKER_THREADS).build()
        );
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createIndex("test", indexSettings(between(1, 5), 0).build());
        createIndex("other", indexSettings(1, 0).build());
        int docs = between(1, 100);
        long sum = 0;
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(prepareIndex("test").setSource("value", i));
            sum += i;
        }
        bulk.get();

        SearchService searchService = internalCluster().getInstance(SearchService.class, dataNode);
        Executor workers = internalCluster().getInstance(ThreadPool.class, dataNode).executor(EsqlPlugin.computePool());
        CountDownLatch blocked = new CountDownLatch(WORKER_THREADS);
        CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < WORKER_THREADS; i++) {
            workers.execute(() -> {
                blocked.countDown();
                safeAwait(release);
            });
        }
        safeAwait(blocked);

        ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM test | STATS s = SUM(value)")
        );
        try {
            // the drivers of the query are queued behind the blocked threads, as one query
            assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(1)));
            // the waiting shows while the drivers are still queued
            assertBusy(() -> assertThat(searchWaiting("test"), greaterThan(0.0)));
        } finally {
            release.countDown();
        }
        try (EsqlQueryResponse response = future.actionGet()) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(sum))));
        }
        assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(0)));

        assertThat(searchWaiting("test"), greaterThan(0.0));
        assertThat(searchWaiting("other"), equalTo(0.0));
    }

    private static double searchWaiting(String index) {
        return indicesAdmin().prepareStats(index)
            .setSearch(true)
            .get()
            .getIndex(index)
            .getTotal()
            .getSearch()
            .getTotal()
            .getSearchWaitingRate();
    }
}
