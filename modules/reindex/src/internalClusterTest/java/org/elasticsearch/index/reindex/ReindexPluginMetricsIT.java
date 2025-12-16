/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.BulkIndexByScrollResponseMatcher;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.reindex.DeleteByQueryMetrics.DELETE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SOURCE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_COMPLETION_COUNTER;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.UpdateByQueryMetrics.UPDATE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexPluginMetricsIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, TestTelemetryPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
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

    public void testReindexFromRemoteMetrics() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remote = new RemoteInfo(
            "http",
            remoteAddress.getHostString(),
            remoteAddress.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        var expectedException = assertThrows(
            "Source index not created yet, should throw not found exception",
            ElasticsearchStatusException.class,
            () -> reindex().source("source").setRemoteInfo(remote).destination("dest").get()
        );

        // assert failure metrics
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE), equalTo(expectedException.status().name()));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_REMOTE));
        });

        // now create the source index
        indexRandom(true, prepareIndex("source").setId("1").setSource("foo", "a"));
        assertHitCount(prepareSearch("source").setSize(0), 1);

        reindex().source("source").setRemoteInfo(remote).destination("dest").get();

        // assert success metrics
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(2));
            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(2));
            assertNull(completions.get(1).attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
            assertThat(completions.get(1).attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_REMOTE));
        });

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
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(1));
        });

        // Now none of them
        createIndex("none");
        reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(2));
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(2));
        });

        // Now half of them
        reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(3));
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(3));
        });

        // Limit with maxDocs
        reindex().source("source").destination("dest_size_one").maxDocs(1).get();
        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(4));
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(4));

            // asset all metric attributes are correct
            testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).forEach(m -> {
                assertNull(m.attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
                assertThat(m.attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
            });
        });
    }

    public void testReindexMetricsWithBulkFailure() throws Exception {
        final String dataNodeName = internalCluster().startNode();
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // source and destination have conflicting mappings to cause bulk failures
        indexRandom(true, prepareIndex("source").setId("2").setSource("test", "words words"));
        indexRandom(true, prepareIndex("dest").setId("1").setSource("test", 10));

        var response = reindex().source("source").destination("dest").get();
        assertThat(response.getBulkFailures().size(), greaterThanOrEqualTo(1));

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(1));
            assertThat(
                testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER)
                    .getFirst()
                    .attributes()
                    .get(ATTRIBUTE_NAME_ERROR_TYPE),
                equalTo("org.elasticsearch.index.mapper.DocumentParsingException")
            );
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
