/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@LuceneTestCase.SuppressCodecs("*")
public class TsidPrefixHistogramIT extends AbstractEsqlIntegTestCase {
    private static final String[] INDICES = { "ts-1", "ts-2" };
    private static final Map<String, OriginalIndices> LOCAL_INDICES = Map.of(
        RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
        new OriginalIndices(new String[] { "ts-*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
    );
    private static final List<String> METRICS = List.of("cpu", "memory");
    private static final int HOSTS = 50;
    private static final int POINTS = 20;
    private static final long START = 1_700_000_000_000L;

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void indexDocs() {
        for (String index : INDICES) {
            Settings settings = Settings.builder()
                .put("mode", "time_series")
                .putList("index.dimensions", List.of("host"))
                .put("number_of_shards", between(1, 3))
                .put("number_of_replicas", 0)
                .build();
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge",
                    "memory",
                    "type=long,time_series_metric=gauge"
                )
                .get();
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int host = 0; host < HOSTS; host++) {
                String metric = host % 2 == 0 ? "cpu" : "memory";
                for (int point = 0; point < POINTS; point++) {
                    bulk.add(
                        new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                            .source("@timestamp", START + point * 1000L, "host", "host-" + host, metric, host + point)
                    );
                }
            }
            assertFalse(bulk.get().hasFailures());
            // one segment per shard: a series cut into two segments covering the same time range would count twice
            assertNoFailures(client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get());
        }
    }

    public void testCollectStats() {
        // both indices hold every host over the same time range, so each host counts once per index
        TsidPrefixHistogram all = collectStats(METRICS, 0);
        assertThat(all.minTimestamp(), equalTo(START));
        assertThat(all.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));
        assertThat(all.estimatedSeries(), equalTo(2L * HOSTS));

        TsidPrefixHistogram cpu = collectStats(List.of("cpu"), 0);
        // all hosts share the first tsid byte, so a memory host sharing a prefix cell with a cpu host counts too
        assertThat(cpu.estimatedSeries(), greaterThanOrEqualTo((long) HOSTS));
        assertThat(cpu.estimatedSeries(), lessThan(all.estimatedSeries()));

        // every series reports in four 5s buckets; the estimate stays within two extra buckets per series
        long groups = 4L * 2 * HOSTS;
        TsidPrefixHistogram buckets = collectStats(METRICS, 5000);
        assertThat(buckets.estimatedGroups(5000), greaterThanOrEqualTo(groups));
        assertThat(buckets.estimatedGroups(5000), lessThanOrEqualTo(groups + 2 * 2L * HOSTS));

        TsidPrefixHistogram missing = collectStats(List.of("unknown"), 0);
        assertThat(missing.totalPrefixes(), equalTo(0));
        assertThat(missing.estimatedSeries(), equalTo(0L));

        TsidPrefixHistogram clipped = collectStats(METRICS, START + 5000, START + 10_000, 0);
        assertThat(clipped.minTimestamp(), equalTo(START + 5000));
        assertThat(clipped.maxTimestamp(), equalTo(START + 10_000));
        assertThat(clipped.estimatedSeries(), equalTo(2L * HOSTS));
    }

    public void testUnassignedShardIsReported() {
        int shards = between(1, 3);
        client().admin()
            .indices()
            .prepareCreate("ts-unassigned")
            .setSettings(
                Settings.builder()
                    .put("mode", "time_series")
                    .putList("index.dimensions", List.of("host"))
                    .put("number_of_shards", shards)
                    .put("number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", "no-such-node")
            )
            .setMapping("@timestamp", "type=date", "host", "type=keyword,time_series_dimension=true")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .get();
        TsidPrefixHistogramResponse response = collect(
            new String[] { "ts-1", "ts-unassigned" },
            METRICS,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0
        );
        assertThat(response.getFailedShards(), equalTo(shards));
        assertThat(response.getShardFailures().size(), equalTo(shards));
        assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards() - shards));
        // ts-* also matches ts-2, but only the concrete indices are collected
        assertThat(response.histogram().estimatedSeries(), equalTo((long) HOSTS));
    }

    private TsidPrefixHistogram collectStats(List<String> metricFields, long bucketMillis) {
        return collectStats(metricFields, Long.MIN_VALUE, Long.MAX_VALUE, bucketMillis);
    }

    private TsidPrefixHistogram collectStats(List<String> metricFields, long minTimestamp, long maxTimestamp, long bucketMillis) {
        TsidPrefixHistogramResponse response = collect(INDICES, metricFields, minTimestamp, maxTimestamp, bucketMillis);
        assertThat(response.getShardFailures().toString(), response.getFailedShards(), equalTo(0));
        return response.histogram();
    }

    private TsidPrefixHistogramResponse collect(
        String[] concreteIndices,
        List<String> metricFields,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis
    ) {
        PlainActionFuture<TsidPrefixHistogramResponse> future = new PlainActionFuture<>();
        var transport = internalCluster().getInstance(TransportService.class);
        var taskManager = transport.getTaskManager();
        var queryTask = asInstanceOf(
            CancellableTask.class,
            taskManager.register("transport", EsqlQueryAction.NAME, syncEsqlQueryRequest("TS ts-*"))
        );
        try {
            TsidPrefixHistogramCollector.collect(
                transport,
                queryTask,
                Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, concreteIndices),
                LOCAL_INDICES,
                metricFields,
                minTimestamp,
                maxTimestamp,
                bucketMillis,
                future
            );
            return safeGet(future);
        } finally {
            taskManager.unregister(queryTask);
        }
    }
}
