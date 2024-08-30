/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.BulkIndexByScrollResponseMatcher;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.reindex.DeleteByQueryMetrics.DELETE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.UpdateByQueryMetrics.UPDATE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexPluginMetricsIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, TestTelemetryPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client());
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return new UpdateByQueryRequestBuilder(client());
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return new DeleteByQueryRequestBuilder(client());
    }

    public static BulkIndexByScrollResponseMatcher matcher() {
        return new BulkIndexByScrollResponseMatcher();
    }

    public void testReindexMetrics() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "a"),
            prepareIndex("source").setId("3").setSource("foo", "b"),
            prepareIndex("source").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Copy all the docs
        reindex().source("source").destination("dest").get();
        // Use assertBusy to wait for all threads to complete so we get deterministic results
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Now none of them
        createIndex("none");
        reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Now half of them
        reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Limit with maxDocs
        reindex().source("source").destination("dest_size_one").maxDocs(1).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }

    public void testDeleteByQueryMetrics() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "a"),
            prepareIndex("test").setId("3").setSource("foo", "b"),
            prepareIndex("test").setId("4").setSource("foo", "c"),
            prepareIndex("test").setId("5").setSource("foo", "d"),
            prepareIndex("test").setId("6").setSource("foo", "e"),
            prepareIndex("test").setId("7").setSource("foo", "f")
        );

        assertHitCount(prepareSearch("test").setSize(0), 7);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Deletes two docs that matches "foo:a"
        deleteByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Deletes the two first docs with limit by size
        DeleteByQueryRequestBuilder request = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).size(2).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        request.get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Deletes but match no docs
        deleteByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Deletes all remaining docs
        deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }

    public void testUpdateByQueryMetrics() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "a"),
            prepareIndex("test").setId("3").setSource("foo", "b"),
            prepareIndex("test").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("test").setSize(0), 4);
        assertEquals(1, client().prepareGet("test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "4").get().getVersion());

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Reindex all the docs
        updateByQuery().source("test").refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Now none of them
        updateByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Now half of them
        updateByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Limit with size
        UpdateByQueryRequestBuilder request = updateByQuery().source("test").size(3).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        request.get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }
}
