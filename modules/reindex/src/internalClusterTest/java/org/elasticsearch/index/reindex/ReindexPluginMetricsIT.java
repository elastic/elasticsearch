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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.BulkByScrollSearchContextMetrics;
import org.elasticsearch.reindex.BulkIndexByScrollResponseMatcher;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.ReindexSettings;
import org.elasticsearch.reindex.Reindexer;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_NAME_SEARCH_SOURCE;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_NAME_TASK_KIND;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL;
import static org.elasticsearch.reindex.BulkByScrollSearchContextMetrics.SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER;
import static org.elasticsearch.reindex.DeleteByQueryMetrics.DELETE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_NAME_SOURCE;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL;
import static org.elasticsearch.reindex.ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_REMOTE;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_COMPLETION_COUNTER;
import static org.elasticsearch.reindex.ReindexMetrics.REINDEX_TIME_HISTOGRAM;
import static org.elasticsearch.reindex.UpdateByQueryMetrics.UPDATE_BY_QUERY_TIME_HISTOGRAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexPluginMetricsIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ReindexPlugin.class,
            TestTelemetryPlugin.class,
            MainRestPlugin.class,
            SearchContextFailureInjectionPlugin.class
        );
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
            // Disables the thread pool’s cached wall‑clock (default ~200ms ticks) so ThreadPool#absoluteTimeInMillis
            // (used as the clock for SearchContextKeepaliveDeadline) advances every call.
            // This keeps short keep‑alive timing tests deterministic.
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)
            .build();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            // Tests that skip via assumeTrue before starting a node leave the cluster empty;
            // the persistent setting reset would then fail with "no node found".
            if (internalCluster().size() > 0) {
                assertAcked(
                    clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                        .setPersistentSettings(Settings.builder().putNull(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()).build())
                );
            }
        } finally {
            SearchContextFailureInjectionPlugin.CONFIG.set(null);
            SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
            SearchContextFailureInjectionPlugin.SCROLL_SEARCH_COUNTER.set(0);
            super.tearDown();
        }
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
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
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
            assertThat(completions.get(1).attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
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
                assertThat(m.attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
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
            Measurement completion = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).getFirst();
            assertThat(
                completion.attributes().get(ATTRIBUTE_NAME_ERROR_TYPE),
                equalTo("org.elasticsearch.index.mapper.DocumentParsingException")
            );
            assertThat(completion.attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
        });
    }

    public void testReindexMetricsWithFixedSlices() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "b"),
            prepareIndex("source").setId("3").setSource("foo", "c"),
            prepareIndex("source").setId("4").setSource("foo", "d")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        reindex().source("source").destination("dest_fixed").setSlices(2).get();

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> histograms = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(histograms.size(), equalTo(1));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("fixed"));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));

            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertNull(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("fixed"));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
        });
    }

    public void testReindexMetricsWithManualSlices() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "b"),
            prepareIndex("source").setId("3").setSource("foo", "c"),
            prepareIndex("source").setId("4").setSource("foo", "d")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        ReindexRequestBuilder request = reindex().source("source").destination("dest_manual");
        request.source().slice(new SliceBuilder(0, 2));
        request.get();

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> histograms = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(histograms.size(), equalTo(1));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("manual"));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));

            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertNull(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("manual"));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
        });
    }

    public void testReindexMetricsWithAutoSlices() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "b"),
            prepareIndex("source").setId("3").setSource("foo", "c"),
            prepareIndex("source").setId("4").setSource("foo", "d")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        reindex().source("source").destination("dest_auto").setSlices(AbstractBulkByScrollRequest.AUTO_SLICES).get();

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            List<Measurement> histograms = testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM);
            assertThat(histograms.size(), equalTo(1));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("auto"));
            assertThat(histograms.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));

            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertNull(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SLICING_MODE), equalTo("auto"));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
        });
    }

    /**
     * Verifies that remote reindex metrics record failures when the remote version lookup fails
     * (e.g. connection refused, host unreachable).
     */
    public void testRemoteReindexVersionLookupFailureMetrics() throws Exception {
        assumeTrue("PIT search must be enabled for remote version lookup path", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final String dataNodeName = internalCluster().startNode();

        // Use an invalid host so version lookup fails during connection
        RemoteInfo invalidRemote = new RemoteInfo(
            "http",
            "invalid.invalid",
            9200,
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            TimeValue.timeValueMillis(100),
            TimeValue.timeValueMillis(100)
        );

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        expectThrows(Exception.class, () -> reindex().source("source").setRemoteInfo(invalidRemote).destination("dest").get());

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE), notNullValue());
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_REMOTE));
        });
    }

    /**
     * Verifies that no reindex metrics are recorded when validation fails before the {@link Reindexer} runs
     * (e.g. source index does not exist).
     */
    public void testLocalReindexValidationFailureNoMetrics() {
        final String dataNodeName = internalCluster().startNode();

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        expectThrows(Exception.class, () -> reindex().source("non_existent_index").destination("dest").get());

        testTelemetryPlugin.collect();
        assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(0));
        assertThat(testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER).size(), equalTo(0));
    }

    /**
     * Verifies that local reindex metrics record failures when PIT open fails (e.g. source index is closed).
     */
    public void testLocalReindexPitOpenFailureMetrics() throws Exception {
        assumeTrue("PIT search must be enabled for local PIT path", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final String dataNodeName = internalCluster().startNode();

        // Create and close the source index so PIT open fails (validation passes because index exists)
        indexRandom(true, prepareIndex("source").setId("1").setSource("foo", "a"));
        indicesAdmin().prepareClose("source").get();

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Use STRICT_EXPAND_OPEN_CLOSED so validation resolves the closed index; PIT open will still fail
        ReindexRequestBuilder builder = reindex().source("source").destination("dest");
        builder.source().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);
        expectThrows(Exception.class, () -> builder.get());

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE), notNullValue());
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
        });
    }

    /**
     * Verifies reindex metrics for a successful local reindex.
     * Uses the scroll path when PIT is disabled, or the PIT path when PIT is enabled.
     */
    public void testLocalReindexMetrics() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(true, prepareIndex("source").setId("1").setSource("foo", "a"), prepareIndex("source").setId("2").setSource("foo", "b"));
        assertHitCount(prepareSearch("source").setSize(0), 2);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        reindex().source("source").destination("dest").get();

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongHistogramMeasurement(REINDEX_TIME_HISTOGRAM).size(), equalTo(1));
            List<Measurement> completions = testTelemetryPlugin.getLongCounterMeasurement(REINDEX_COMPLETION_COUNTER);
            assertThat(completions.size(), equalTo(1));
            assertNull(completions.getFirst().attributes().get(ATTRIBUTE_NAME_ERROR_TYPE));
            assertThat(completions.getFirst().attributes().get(ATTRIBUTE_NAME_SOURCE), equalTo(ATTRIBUTE_VALUE_SOURCE_LOCAL));
        });
    }

    /**
     * Successful local reindex must not increment {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER}.
     * That counter is reserved for when the underlying search context's keep-alive expires
     */
    public void testSuccessfulLocalReindexDoesNotEmitSearchContextKeepaliveExpiredMetric() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "b"),
            prepareIndex("source").setId("3").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("source").setSize(0), 3);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        BulkByScrollResponse response = reindex().source("source").destination("dest").refresh(true).get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertHitCount(prepareSearch("dest").setSize(0), 3);

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            final long keepaliveExpiredTotal = testTelemetryPlugin.getLongCounterMeasurement(
                BulkByScrollSearchContextMetrics.SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER
            ).stream().mapToLong(Measurement::getLong).sum();
            assertThat(keepaliveExpiredTotal, equalTo(0L));
        });
    }

    /**
     * Successful update by query must not increment {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER}.
     * That counter is reserved for when the underlying search context's keep-alive expires
     */
    public void testSuccessfulUpdateByQueryDoesNotEmitSearchContextKeepaliveExpiredMetric() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "b"),
            prepareIndex("test").setId("3").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("test").setSize(0), 3);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        BulkByScrollResponse response = updateByQuery().source("test").refresh(true).get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertHitCount(prepareSearch("test").setSize(0), 3);

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            final long keepaliveExpiredTotal = testTelemetryPlugin.getLongCounterMeasurement(
                BulkByScrollSearchContextMetrics.SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER
            ).stream().mapToLong(Measurement::getLong).sum();
            assertThat(keepaliveExpiredTotal, equalTo(0L));
        });
    }

    /**
     * Successful delete by query must not increment {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER}.
     * That counter is reserved for when the underlying search context's keep-alive expires
     */
    public void testSuccessfulDeleteByQueryDoesNotEmitSearchContextKeepaliveExpiredMetric() throws Exception {
        final String dataNodeName = internalCluster().startNode();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", "a"),
            prepareIndex("test").setId("2").setSource("foo", "b"),
            prepareIndex("test").setId("3").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("test").setSize(0), 3);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        BulkByScrollResponse response = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertHitCount(prepareSearch("test").setSize(0), 0);

        assertBusy(() -> {
            testTelemetryPlugin.collect();
            final long keepaliveExpiredTotal = testTelemetryPlugin.getLongCounterMeasurement(
                BulkByScrollSearchContextMetrics.SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER
            ).stream().mapToLong(Measurement::getLong).sum();
            assertThat(keepaliveExpiredTotal, equalTo(0L));
        });
    }

    /**
     * A missing PIT search context that fails after the keep-alive deadline must increment
     * {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER} for local reindex.
     * Uses {@link SearchContextFailureInjectionPlugin}: sets a short PIT keep-alive, sleeps on the second pit search,
     * then fails with {@link SearchContextMissingException}.
     */
    public void testReindexEmitsSearchContextKeepaliveExpiredMetric() throws Exception {
        assumeTrue("PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final String dataNodeName = internalCluster().startNode();
        TimeValue pitKeepAlive = TimeValue.timeValueMillis(200);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), pitKeepAlive.getStringRep()).build()
                )
        );
        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("n", 1),
            prepareIndex("source").setId("2").setSource("n", 2),
            prepareIndex("source").setId("3").setSource("n", 3)
        );
        assertHitCount(prepareSearch("source").setSize(0).setTrackTotalHits(true), 3L);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.SCROLL_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.CONFIG.set(
            new SearchContextFailureInjectionPlugin.InjectionConfig(
                TimeValue.timeValueMillis(pitKeepAlive.millis() + 100),
                new SearchContextMissingException(new ShardSearchContextId("metrics_reindex", 1L))
            )
        );

        ReindexRequestBuilder builder = reindex().source("source").destination("dest");
        builder.source().setSize(1);
        expectThrows(Exception.class, () -> builder.get());

        assertSearchContextKeepaliveExpiredMetric(testTelemetryPlugin, BulkByScrollSearchContextMetrics.TaskKind.REINDEX);
    }

    /**
     * A missing scroll search context after the scroll keep-alive deadline must increment
     * {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER} for update-by-query.
     */
    public void testUpdateByQueryEmitsSearchContextKeepaliveExpiredMetric() throws Exception {
        final String dataNodeName = internalCluster().startNode();
        TimeValue scrollKeepAlive = TimeValue.timeValueMillis(200);
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("n", 1),
            prepareIndex("test").setId("2").setSource("n", 2),
            prepareIndex("test").setId("3").setSource("n", 3)
        );
        assertHitCount(prepareSearch("test").setSize(0).setTrackTotalHits(true), 3L);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.SCROLL_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.CONFIG.set(
            new SearchContextFailureInjectionPlugin.InjectionConfig(
                TimeValue.timeValueMillis(scrollKeepAlive.millis() + 100),
                new SearchContextMissingException(new ShardSearchContextId("metrics_ubq", 1L))
            )
        );

        UpdateByQueryRequestBuilder request = updateByQuery().source("test").refresh(true);
        request.source().setScroll(scrollKeepAlive);
        request.source().setSize(1);
        expectThrows(Exception.class, request::get);

        assertSearchContextKeepaliveExpiredMetric(testTelemetryPlugin, BulkByScrollSearchContextMetrics.TaskKind.UPDATE_BY_QUERY);
    }

    /**
     * A missing scroll search context after the scroll keep-alive deadline must increment
     * {@link BulkByScrollSearchContextMetrics#SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER} for delete-by-query.
     */
    public void testDeleteByQueryEmitsSearchContextKeepaliveExpiredMetric() throws Exception {
        final String dataNodeName = internalCluster().startNode();
        TimeValue scrollKeepAlive = TimeValue.timeValueMillis(200);
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("n", 1),
            prepareIndex("test").setId("2").setSource("n", 2),
            prepareIndex("test").setId("3").setSource("n", 3)
        );
        assertHitCount(prepareSearch("test").setSize(0).setTrackTotalHits(true), 3L);

        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.SCROLL_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.CONFIG.set(
            new SearchContextFailureInjectionPlugin.InjectionConfig(
                TimeValue.timeValueMillis(scrollKeepAlive.millis() + 100),
                new SearchContextMissingException(new ShardSearchContextId("metrics_dbq", 1L))
            )
        );

        DeleteByQueryRequestBuilder request = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true);
        request.source().setScroll(scrollKeepAlive);
        request.source().setSize(1);
        expectThrows(Exception.class, request::get);

        assertSearchContextKeepaliveExpiredMetric(testTelemetryPlugin, BulkByScrollSearchContextMetrics.TaskKind.DELETE_BY_QUERY);
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

    private void assertSearchContextKeepaliveExpiredMetric(
        TestTelemetryPlugin plugin,
        BulkByScrollSearchContextMetrics.TaskKind expectedTaskKind
    ) throws Exception {
        assertBusy(() -> {
            plugin.collect();
            long total = plugin.getLongCounterMeasurement(SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER)
                .stream()
                .filter(
                    m -> expectedTaskKind.attributeValue().equals(m.attributes().get(ATTRIBUTE_NAME_TASK_KIND))
                        && ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL.equals(m.attributes().get(ATTRIBUTE_NAME_SEARCH_SOURCE))
                )
                .mapToLong(Measurement::getLong)
                .sum();
            assertThat(total, greaterThanOrEqualTo(1L));
        });
    }
}
