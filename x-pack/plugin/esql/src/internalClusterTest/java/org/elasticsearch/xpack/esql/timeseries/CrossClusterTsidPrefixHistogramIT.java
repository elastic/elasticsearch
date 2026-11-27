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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.SuppressCodecs("*")
public class CrossClusterTsidPrefixHistogramIT extends AbstractCrossClusterTestCase {
    private static final String INDEX = "ts";
    private static final int LOCAL_HOSTS = 30;
    private static final int REMOTE_HOSTS = 20;
    private static final int POINTS = 10;
    private static final long START = 1_700_000_000_000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false);
    }

    public void testCollectStatsAcrossClusters() {
        populate(LOCAL_CLUSTER, LOCAL_HOSTS);
        populate(REMOTE_CLUSTER_1, REMOTE_HOSTS);

        assertThat(collectStats(LOCAL_CLUSTER).estimatedSeries(), equalTo((long) LOCAL_HOSTS));
        assertThat(collectStats(REMOTE_CLUSTER_1).estimatedSeries(), equalTo((long) REMOTE_HOSTS));

        TsidPrefixHistogram both = collectStats(LOCAL_CLUSTER, REMOTE_CLUSTER_1);
        assertThat(both.minTimestamp(), equalTo(START));
        assertThat(both.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));
        assertThat(both.estimatedSeries(), equalTo((long) LOCAL_HOSTS + REMOTE_HOSTS));
        assertThat(collectStats(LOCAL_CLUSTER, "unavailable").estimatedSeries(), equalTo((long) LOCAL_HOSTS));
    }

    private void populate(String clusterAlias, int hosts) {
        Client client = client(clusterAlias);
        client.admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(
                Settings.builder()
                    .put("mode", "time_series")
                    .put("routing_path", "host")
                    .put("number_of_shards", between(1, 3))
                    .put("number_of_replicas", 0)
            )
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int host = 0; host < hosts; host++) {
            for (int point = 0; point < POINTS; point++) {
                bulk.add(
                    new IndexRequest(INDEX).opType(DocWriteRequest.OpType.CREATE)
                        .source("@timestamp", START + point * 1000L, "host", "host-" + host, "cpu", host + point)
                );
            }
        }
        assertFalse(bulk.get().hasFailures());
        // one segment per shard, so each series counts once
        assertNoFailures(client.admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
    }

    private TsidPrefixHistogram collectStats(String... clusterAliases) {
        Map<String, String[]> concreteIndices = new HashMap<>();
        Map<String, OriginalIndices> originalIndices = new HashMap<>();
        for (String clusterAlias : clusterAliases) {
            // field caps names remote indices with their cluster prefix
            concreteIndices.put(clusterAlias, new String[] { clusterAlias.isEmpty() ? INDEX : clusterAlias + ":" + INDEX });
            originalIndices.put(clusterAlias, new OriginalIndices(new String[] { INDEX }, SearchRequest.DEFAULT_INDICES_OPTIONS));
        }
        var transport = cluster(LOCAL_CLUSTER).getInstance(TransportService.class);
        var taskManager = transport.getTaskManager();
        var queryTask = asInstanceOf(
            CancellableTask.class,
            taskManager.register("transport", EsqlQueryAction.NAME, syncEsqlQueryRequest("TS " + INDEX))
        );
        try {
            var future = new PlainActionFuture<TsidPrefixHistogramResponse>();
            TsidPrefixHistogramCollector.collect(
                transport,
                queryTask,
                concreteIndices,
                originalIndices,
                List.of("cpu"),
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                0,
                future
            );
            TsidPrefixHistogramResponse response = safeGet(future);
            assertThat(response.getShardFailures().toString(), response.getFailedShards(), equalTo(0));
            return response.histogram();
        } finally {
            taskManager.unregister(queryTask);
        }
    }
}
