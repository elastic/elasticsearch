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
        internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "a"),
            prepareIndex("source").setId("3").setSource("foo", "b"),
            prepareIndex("source").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        final String dataNodeName = internalCluster().getRandomNodeName();
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        assertThat(copy.get(), matcher().created(4));
        assertHitCount(prepareSearch("dest").setSize(0), 4);

        // Use assertBusy to wait for all threads to complete so we get deterministic results
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Now none of them
        createIndex("none");
        copy = reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).refresh(true);
        assertThat(copy.get(), matcher().created(0));
        assertHitCount(prepareSearch("none").setSize(0), 0);
        // wait for all threads to complete so that we get deterministic results
        // waitUntil(() -> (tps[0] = tp.stats()).stats().stream().allMatch(s -> s.active() == 0));

        // testTelemetryPlugin.collect();
        // measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Now half of them
        copy = reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).refresh(true);
        assertThat(copy.get(), matcher().created(2));
        assertHitCount(prepareSearch("dest_half").setSize(0), 2);

        // wait for all threads to complete so that we get deterministic results
        // waitUntil(() -> (tps[0] = tp.stats()).stats().stream().allMatch(s -> s.active() == 0));

        // testTelemetryPlugin.collect();
        // measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Limit with maxDocs
        copy = reindex().source("source").destination("dest_size_one").maxDocs(1).refresh(true);
        assertThat(copy.get(), matcher().created(1));
        assertHitCount(prepareSearch("dest_size_one").setSize(0), 1);

        // wait for all threads to complete so that we get deterministic results
        // waitUntil(() -> (tps[0] = tp.stats()).stats().stream().allMatch(s -> s.active() == 0));

        // testTelemetryPlugin.collect();
        // measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }

    public void testDeleteByQueryMetrics() throws Exception {
        internalCluster().startNode();

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

        final String dataNodeName = internalCluster().getRandomNodeName();
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Deletes two docs that matches "foo:a"
        assertThat(deleteByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get(), matcher().deleted(2));
        assertHitCount(prepareSearch("test").setSize(0), 5);

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Deletes the two first docs with limit by size
        DeleteByQueryRequestBuilder request = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).size(2).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        assertThat(request.get(), matcher().deleted(2));
        assertHitCount(prepareSearch("test").setSize(0), 3);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Deletes but match no docs
        assertThat(deleteByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), matcher().deleted(0));
        assertHitCount(prepareSearch("test").setSize(0), 3);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Deletes all remaining docs
        assertThat(deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get(), matcher().deleted(3));
        assertHitCount(prepareSearch("test").setSize(0), 0);
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(DELETE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }

    public void testUpdateByQueryMetrics() throws Exception {
        internalCluster().startNode();

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

        final String dataNodeName = internalCluster().getRandomNodeName();
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Reindex all the docs
        assertThat(updateByQuery().source("test").refresh(true).get(), matcher().updated(4));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(1));
        });

        // Now none of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), matcher().updated(0));
        assertEquals(2, client().prepareGet("test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(2));
        });

        // Now half of them
        assertThat(updateByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get(), matcher().updated(2));
        assertEquals(3, client().prepareGet("test", "1").get().getVersion());
        assertEquals(3, client().prepareGet("test", "2").get().getVersion());
        assertEquals(2, client().prepareGet("test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(3));
        });

        // Limit with size
        UpdateByQueryRequestBuilder request = updateByQuery().source("test").size(3).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        assertThat(request.get(), matcher().updated(3));
        // Only the first three documents are updated because of sort
        assertEquals(4, client().prepareGet("test", "1").get().getVersion());
        assertEquals(4, client().prepareGet("test", "2").get().getVersion());
        assertEquals(3, client().prepareGet("test", "3").get().getVersion());
        assertEquals(2, client().prepareGet("test", "4").get().getVersion());
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> measurements = testTelemetryPlugin.getLongHistogramMeasurement(UPDATE_BY_QUERY_TIME_HISTOGRAM);
            assertThat(measurements.size(), equalTo(4));
        });
    }
}
