/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.scheduling.QueryLoad;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.StreamQueryTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The time ES|QL spends on a query is charged to the shards it reads, on the nodes holding them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlSearchLoadIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AcquisitionsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // no reaper of search contexts on the search pool while the queries run
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1))
            .build();
    }

    public void testChargesTheIndicesTheQueryReads() throws Exception {
        internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(prepareCreate("read").setSettings(indexSettings(between(1, 3), 0)).setMapping("k", "type=keyword", "v", "type=long"));
        assertAcked(prepareCreate("other").setSettings(indexSettings(1, 0)).setMapping("k", "type=keyword", "v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 1_000; i++) {
            bulk.add(prepareIndex(randomFrom("read", "other")).setSource("k", "k" + (i % 10), "v", i));
        }
        assertNoFailures(bulk.get());

        for (int i = between(1, 5); i > 0; i--) {
            var request = syncEsqlQueryRequest(randomFrom("FROM read | STATS c = COUNT(*) BY k", "FROM read | SORT v DESC | LIMIT 10"));
            if (randomBoolean()) {
                StreamQueryTestUtils.executeStreamRequest(
                    client(coordinator),
                    request,
                    new StreamQueryTestUtils.CountingStreamSubscriber()
                );
            } else {
                try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
                    assertNotNull(response);
                }
            }
        }

        // the data node charges the shards it read
        assertThat(searchLoad("read"), greaterThan(0.0));
        assertCpuIsPartOfTheLoad("read");
        assertThat(searchLoad("other"), equalTo(0.0));
    }

    /**
     * On a data node, every driver task on the worker pool, including the node-level reduce, and acquiring the search
     * contexts on the search pool charge the shards the queries read, once. The transport glue on the search pool charges
     * nothing: the requests that start the drivers, their responses and the exchange with the coordinator. The queries read
     * all the docs they match, since a driver that finishes early can run outside the pools.
     */
    public void testDataNodeTasksChargeTheShardsTheyRead() throws Exception {
        String dataNode = internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(prepareCreate("read").setSettings(indexSettings(between(2, 4), 0)).setMapping("k", "type=keyword", "v", "type=long"));
        assertAcked(prepareCreate("other").setSettings(indexSettings(1, 0)).setMapping("k", "type=keyword", "v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 1_000; i++) {
            bulk.add(prepareIndex(randomFrom("read", "other")).setSource("k", "k" + (i % 10), "v", i));
        }
        assertNoFailures(bulk.get());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, dataNode);
        var search = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        var workers = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(EsqlPlugin.computePool());
        long searchAttributedBefore = search.getAttributedTaskExecutionTime();
        long workersAttributedBefore = workers.getAttributedTaskExecutionTime();
        long workersTotalBefore = workers.getTotalTaskExecutionTime();
        long chargedBefore = chargedToShards(dataNode);
        long reported = 0;

        for (int i = between(1, 5); i > 0; i--) {
            String query = randomFrom(
                "FROM read | STATS c = COUNT(*) BY k",
                "FROM read | SORT v DESC | LIMIT 10",
                "FROM read | WHERE v > 100 | KEEP k, v | LIMIT 1000"
            );
            try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
                assertThat(response.searchLoad().loadNanos(), greaterThan(0L));
                assertThat(response.searchLoad().cpuNanos(), lessThanOrEqualTo(response.searchLoad().loadNanos()));
                reported += response.searchLoad().loadNanos();
            }
        }

        assertBusy(() -> {
            long workersTotal = workers.getTotalTaskExecutionTime() - workersTotalBefore;
            long workersAttributed = workers.getAttributedTaskExecutionTime() - workersAttributedBefore;
            long searchAttributed = search.getAttributedTaskExecutionTime() - searchAttributedBefore;
            assertThat(workersTotal, greaterThan(0L));
            assertThat(searchAttributed, greaterThan(0L));
            // every driver task is charged
            assertThat(workersAttributed, equalTo(workersTotal));
            // and all that is charged goes to the shards, once
            assertThat(chargedToShards(dataNode) - chargedBefore, equalTo(workersAttributed + searchAttributed));
        });
        // the shards are also charged for tasks still running when the queries complete
        assertThat(reported, lessThanOrEqualTo(chargedToShards(dataNode) - chargedBefore));
        assertThat(searchLoad("read"), greaterThan(0.0));
        assertCpuIsPartOfTheLoad("read");
        assertThat(searchLoad("other"), equalTo(0.0));
    }

    /**
     * A batch whose only search context fails to open completes on the task that acquired it, which then acquires the next
     * batch. That acquisition is charged to the next batch.
     */
    public void testNextBatchAcquiredOnTheSameTaskIsChargedToItsBatch() {
        internalCluster().startNode();
        AcquisitionsPlugin.ACQUISITIONS.clear();
        // an explicit refresh interval, so that the shards never wait for a refresh and acquire on the same task
        Settings settings = indexSettings(1, 0).put("index.refresh_interval", "-1").build();
        // without @timestamp, its shard runs first
        assertAcked(prepareCreate(AcquisitionsPlugin.FAILS_TO_OPEN).setSettings(settings).setMapping("v", "type=long"));
        assertAcked(prepareCreate(AcquisitionsPlugin.OPENS).setSettings(settings).setMapping("@timestamp", "type=date", "v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(prepareIndex(AcquisitionsPlugin.FAILS_TO_OPEN).setSource("v", 1));
        bulk.add(prepareIndex(AcquisitionsPlugin.OPENS).setSource("@timestamp", "2026-01-01", "v", 2));
        assertNoFailures(bulk.get());

        var request = syncEsqlQueryRequest("FROM " + AcquisitionsPlugin.FAILS_TO_OPEN + "," + AcquisitionsPlugin.OPENS + " | LIMIT 10");
        request.allowPartialResults(true);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (var response = client().execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
            assertTrue(response.isPartial());
        }

        List<Acquisition> acquisitions = List.copyOf(AcquisitionsPlugin.ACQUISITIONS);
        assertThat(
            acquisitions.stream().map(Acquisition::index).toList(),
            contains(AcquisitionsPlugin.FAILS_TO_OPEN, AcquisitionsPlugin.OPENS)
        );
        Acquisition failed = acquisitions.get(0);
        Acquisition opened = acquisitions.get(1);
        assertSame(failed.thread(), opened.thread());
        assertNotSame(failed.charging().load(), opened.charging().load());
    }

    private record Acquisition(String index, Thread thread, QueryLoad.Charging charging) {}

    /**
     * Records the target charged when a search context of its indices acquires its searcher, and fails it for
     * {@link #FAILS_TO_OPEN}.
     */
    public static class AcquisitionsPlugin extends Plugin {
        static final String FAILS_TO_OPEN = "fails_to_open";
        static final String OPENS = "opens";
        static final Queue<Acquisition> ACQUISITIONS = new ConcurrentLinkedQueue<>();

        @Override
        public void onIndexModule(IndexModule module) {
            String index = module.getIndex().getName();
            if (index.equals(FAILS_TO_OPEN) || index.equals(OPENS)) {
                module.setReaderWrapper(indexService -> reader -> {
                    // only the acquisitions of the data node compute run charged to a query load
                    QueryLoad.Charging charging = QueryLoad.Charging.running();
                    if (charging != null) {
                        ACQUISITIONS.add(new Acquisition(index, Thread.currentThread(), charging));
                        if (index.equals(FAILS_TO_OPEN)) {
                            throw new IllegalStateException("fails to open");
                        }
                    }
                    return reader;
                });
            }
        }
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

    /**
     * The CPU time charged to {@code index} is part of its search load.
     */
    private static void assertCpuIsPartOfTheLoad(String index) {
        SearchStats.Stats stats = indicesAdmin().prepareStats(index)
            .setSearch(true)
            .get()
            .getIndex(index)
            .getTotal()
            .getSearch()
            .getTotal();
        assertThat(stats.getSearchCpuRate(), greaterThan(0.0));
        assertThat(stats.getSearchCpuRate(), lessThanOrEqualTo(stats.getSearchLoadRate()));
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
