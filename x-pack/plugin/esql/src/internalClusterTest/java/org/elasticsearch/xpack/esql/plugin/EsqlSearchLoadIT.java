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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.StreamQueryTestUtils;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The time ES|QL spends on a query is charged to the shards it reads, on the nodes holding them.
 * External (CSV/Parquet) computes charge the query the same way, via {@code DriverCompletionInfo}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlSearchLoadIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, ParquetDataSourcePlugin.class);
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
                assertThat(response.searchLoadNanos(), greaterThan(0L));
                assertThat(response.searchCpuNanos(), lessThanOrEqualTo(response.searchLoadNanos()));
                reported += response.searchLoadNanos();
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
     * An external CSV query on a data node reports load and CPU on {@code DriverCompletionInfo}
     * (and the response). Coordinator work is not charged yet; see #166054.
     *
     * <p>{@code AdaptiveStrategy} stays {@code LOCAL} for a single split or a LIMIT-only plan, which
     * runs coordinator {@code "single"} with a null load. Two files plus grouped {@code STATS} fan
     * empty-shard {@code DATA} compute to the data node, the path that charges {@code QueryLoad}.
     */
    public void testExternalCsvOnDataNodeReportsSearchLoad() throws Exception {
        String dataNode = internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        Path dir = createTempDir();
        Files.writeString(dir.resolve("a.csv"), csvRows(1_000));
        Files.writeString(dir.resolve("b.csv"), csvRows(1_000));
        String dataset = registerDataset("csv_load", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));
        assertExternalQueryReportsSearchLoad(dataNode, coordinator, "FROM " + dataset + " | STATS c = COUNT(*) BY k");
    }

    public void testExternalParquetOnDataNodeReportsSearchLoad() throws Exception {
        String dataNode = internalCluster().startNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        Path dir = createTempDir();
        writeParquet(dir.resolve("a.parquet"), 1_000, 128);
        writeParquet(dir.resolve("b.parquet"), 1_000, 128);
        String dataset = registerDataset("parquet_load", StoragePath.fileUri(dir) + "/*.parquet", Map.of());
        assertExternalQueryReportsSearchLoad(dataNode, coordinator, "FROM " + dataset + " | STATS c = COUNT(*) BY name");
    }

    /**
     * Data node: load and CPU on {@code DriverCompletionInfo} (copied onto the response). Coordinator
     * {@code "final"} compute has no load target — #166054 does not charge coordinator work.
     */
    private void assertExternalQueryReportsSearchLoad(String dataNode, String coordinator, String query) throws Exception {
        ThreadPool dataPool = internalCluster().getInstance(ThreadPool.class, dataNode);
        ThreadPool coordPool = internalCluster().getInstance(ThreadPool.class, coordinator);
        var dataWorkers = (TaskExecutionTimeTrackingEsThreadPoolExecutor) dataPool.executor(EsqlPlugin.computePool());
        var coordWorkers = (TaskExecutionTimeTrackingEsThreadPoolExecutor) coordPool.executor(EsqlPlugin.computePool());
        long dataAttributedBefore = dataWorkers.getAttributedTaskExecutionTime();
        long coordAttributedBefore = coordWorkers.getAttributedTaskExecutionTime();

        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
            assertThat(response.searchLoadNanos(), greaterThan(0L));
            assertThat(response.searchCpuNanos(), greaterThan(0L));
            assertThat(response.searchCpuNanos(), lessThanOrEqualTo(response.searchLoadNanos()));
        }

        assertBusy(() -> assertThat(dataWorkers.getAttributedTaskExecutionTime() - dataAttributedBefore, greaterThan(0L)));
        assertThat(coordWorkers.getAttributedTaskExecutionTime() - coordAttributedBefore, equalTo(0L));
    }

    private static String csvRows(int rows) {
        StringBuilder csv = new StringBuilder("k:keyword,v:long\n");
        for (int i = 0; i < rows; i++) {
            csv.append("k").append(i % 10).append(',').append(i).append('\n');
        }
        return csv.toString();
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
