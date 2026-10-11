/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.telemetry;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.CAN_MATCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Multi-node integration tests for the coordinator-level per-request shard response bytes histograms
 * in {@link SearchResponseMetrics}.
 *
 * <p>Uses coordinating only nodes so that real inter-node transport serialization fires.
 * This ensures that the byte counts recorded in the histograms are non-zero and reflect actual
 * deserialized response sizes.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1)
public class SearchCoordinatorPhaseShardBytesMetricsIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "shard_bytes_metrics_it";

    private int numShards;
    private String coordOnlyNode;

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class, ThrowingQueryBuilderPlugin.class);
    }

    public static class ThrowingQueryBuilderPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<SearchPlugin.QuerySpec<?>> getQueries() {
            return List.of(new SearchPlugin.QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("XContent parsing not supported in test");
            }));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return super.nodeSettings(nodeOrdinal, otherSettings);
    }

    @Before
    public void setUpRemoteShardIndex() {
        coordOnlyNode = internalCluster().getNodeNameThat(settings -> DiscoveryNode.canContainData(settings) == false);

        // Two shards so the batched path sends a NodeQueryRequest with size > 1.
        // (A single-shard request falls back to executeAsSingleRequest in the batched path.)
        prepareCreate(INDEX_NAME).setMapping("field", "type=keyword").get();
        ensureGreen(INDEX_NAME);
        GetSettingsResponse getSettingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest(TimeValue.MINUS_ONE))
            .actionGet();
        numShards = Integer.valueOf(getSettingsResponse.getSetting(INDEX_NAME, IndexMetadata.SETTING_NUMBER_OF_SHARDS));

        prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    public void cleanUp() {
        indicesAdmin().prepareDelete(INDEX_NAME).get();
        for (String nodeName : internalCluster().getNodeNames()) {
            getPlugin(nodeName).resetMeter();
        }
    }

    private TestTelemetryPlugin getPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    public void testBatchedQueryThenFetchRecordsNonZeroBytes() {
        assertSearchHitsWithoutFailures(
            client(coordOnlyNode).prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()),
            "1",
            "2"
        );

        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        List<Measurement> queryRequestMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one query request observation per search request", 1, queryRequestMeasurements.size());
        assertThat("batched NodeQueryRequest bytes must be > 0", queryRequestMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> fetchRequestMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one fetch request observation per search request", 1, fetchRequestMeasurements.size());
        assertThat("ShardFetchSearchRequest bytes must be > 0", fetchRequestMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> queryMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one query observation per request", 1, queryMeasurements.size());
        assertThat("batched NodeQueryResponse bytes must be > 0", queryMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> fetchMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one fetch observation per request", 1, fetchMeasurements.size());
        assertThat("fetch response bytes must be > 0", fetchMeasurements.get(0).getLong(), greaterThan(0L));
    }

    public void testNonBatchedQueryThenFetchRecordsNonZeroBytes() {
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), false));
        try {
            assertSearchHitsWithoutFailures(
                client(coordOnlyNode).prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()),
                "1",
                "2"
            );

            TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

            List<Measurement> queryRequestMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertEquals("one query request observation per search request", 1, queryRequestMeasurements.size());
            assertThat("batched NodeQueryRequest bytes must be > 0", queryRequestMeasurements.get(0).getLong(), greaterThan(0L));

            List<Measurement> fetchRequestMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertEquals("one fetch request observation per search request", 1, fetchRequestMeasurements.size());
            assertThat("ShardFetchSearchRequest bytes must be > 0", fetchRequestMeasurements.get(0).getLong(), greaterThan(0L));

            List<Measurement> queryMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertEquals("one query observation per request", 1, queryMeasurements.size());
            assertThat("non-batched QuerySearchResult bytes must be > 0", queryMeasurements.get(0).getLong(), greaterThan(0L));

            List<Measurement> fetchMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertEquals("one fetch observation per request", 1, fetchMeasurements.size());
            assertThat("fetch response bytes must be > 0", fetchMeasurements.get(0).getLong(), greaterThan(0L));
        } finally {
            updateClusterSettings(Settings.builder().putNull(SearchService.BATCHED_QUERY_PHASE.getKey()));
        }
    }

    public void testDfsQueryThenFetchRecordsNonZeroBytes() {
        assertSearchHitsWithoutFailures(
            client(coordOnlyNode).prepareSearch(INDEX_NAME).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(matchAllQuery()),
            "1",
            "2"
        );

        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        List<Measurement> dfsRequestMeasurements = plugin.getLongHistogramMeasurement(DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one dfs request observation per search request", 1, dfsRequestMeasurements.size());
        assertThat("ShardSearchRequest (dfs) bytes must be > 0", dfsRequestMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> dfsQueryRequestMeasurements = plugin.getLongHistogramMeasurement(DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one dfs_query request observation per search request", 1, dfsQueryRequestMeasurements.size());
        assertThat("QuerySearchRequest (dfs_query) bytes must be > 0", dfsQueryRequestMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> fetchRequestMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one fetch request observation per search request", 1, fetchRequestMeasurements.size());
        assertThat("ShardFetchSearchRequest bytes must be > 0", fetchRequestMeasurements.get(0).getLong(), greaterThan(0L));

        assertEquals(0, plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME).size());

        List<Measurement> dfsMeasurements = plugin.getLongHistogramMeasurement(DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one dfs observation per request", 1, dfsMeasurements.size());
        assertThat("DfsSearchResult bytes must be > 0", dfsMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> dfsQueryMeasurements = plugin.getLongHistogramMeasurement(DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one dfs_query observation per request", 1, dfsQueryMeasurements.size());
        assertThat("dfs_query QuerySearchResult bytes must be > 0", dfsQueryMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> fetchMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one fetch observation per request", 1, fetchMeasurements.size());
        assertThat("fetch response bytes must be > 0", fetchMeasurements.get(0).getLong(), greaterThan(0L));

        assertEquals(0, plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME).size());
    }

    public void testCanMatchRecordsNonZeroBytes() {
        assertSearchHitsWithoutFailures(
            client(coordOnlyNode).prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .addSort(fieldSort("_doc")),
            "1",
            "2"
        );

        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        List<Measurement> canMatchMeasurements = plugin.getLongHistogramMeasurement(CAN_MATCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertEquals("one can_match observation per request", 1, canMatchMeasurements.size());
        assertThat("can_match CanMatchNodeResponse bytes must be > 0", canMatchMeasurements.get(0).getLong(), greaterThan(0L));

        List<Measurement> canMatchRequestMeasurements = plugin.getLongHistogramMeasurement(CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertEquals("one can_match request observation per search request", 1, canMatchRequestMeasurements.size());
        assertThat("CanMatchNodeRequest bytes must be > 0", canMatchRequestMeasurements.get(0).getLong(), greaterThan(0L));
    }

    private static long sumHistogram(TestTelemetryPlugin plugin, String... histogramNames) {
        long total = 0;
        for (String name : histogramNames) {
            total += plugin.getLongHistogramMeasurement(name).stream().mapToLong(Measurement::getLong).sum();
        }
        return total;
    }

    /**
     * Field collapsing with {@code inner_hits} triggers the expand search phase,
     * which fans out one sub-search per collapsed group. Each sub-search is an independent
     * {@link org.elasticsearch.action.search.TransportSearchAction} invocation with its own per-phase
     * byte accumulators, so it records its own histogram observations rather than contributing to
     * the parent search's counters.
     */
    public void testFieldCollapsingExpandSearchesAreTrackedSeparately() {
        final String collapseIndex = "collapse_expand_it";
        prepareCreate(collapseIndex).setMapping("group", "type=keyword").get();
        ensureGreen(collapseIndex);
        prepareIndex(collapseIndex).setId("1").setSource("group", "g1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(collapseIndex).setId("2").setSource("group", "g2").setRefreshPolicy(IMMEDIATE).get();
        try {
            assertSearchHitsWithoutFailures(
                client(coordOnlyNode).prepareSearch(collapseIndex)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(matchAllQuery())
                    .setCollapse(new CollapseBuilder("group").setInnerHits(new InnerHitBuilder("ih").setSize(1))),
                "1",
                "2"
            );

            TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

            // parent search = 1 observation; 2 expand sub-searches = 2 more observations; total = 3
            List<Measurement> queryResultMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertEquals(
                "parent + 2 expand sub-searches produce 3 independent query result observations",
                3,
                queryResultMeasurements.size()
            );
            assertThat(
                "query result bytes must be > 0",
                queryResultMeasurements.stream().mapToLong(Measurement::getLong).sum(),
                greaterThan(0L)
            );

            List<Measurement> fetchResultMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertEquals(
                "parent + 2 expand sub-searches produce 3 independent fetch result observations",
                3,
                fetchResultMeasurements.size()
            );
            assertThat(
                "fetch result bytes must be > 0",
                fetchResultMeasurements.stream().mapToLong(Measurement::getLong).sum(),
                greaterThan(0L)
            );

            List<Measurement> queryRequestMeasurements = plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertEquals(
                "parent + 2 expand sub-searches produce 3 independent query request observations",
                3,
                queryRequestMeasurements.size()
            );
            assertThat(
                "query request bytes must be > 0",
                queryRequestMeasurements.stream().mapToLong(Measurement::getLong).sum(),
                greaterThan(0L)
            );

            List<Measurement> fetchRequestMeasurements = plugin.getLongHistogramMeasurement(FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertEquals(
                "parent + 2 expand sub-searches produce 3 independent fetch request observations",
                3,
                fetchRequestMeasurements.size()
            );
            assertThat(
                "fetch request bytes must be > 0",
                fetchRequestMeasurements.stream().mapToLong(Measurement::getLong).sum(),
                greaterThan(0L)
            );
        } finally {
            indicesAdmin().prepareDelete(collapseIndex).get();
        }
    }

    /**
     * Query request and result bytes must grow when aggs are requested
     */
    public void testQueryResultBytesReflectAggregationResults() {
        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        Client client = client(coordOnlyNode);
        assertSearchHitsWithoutFailures(
            client.prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()),
            "1",
            "2"
        );
        long queryResultBytesNoAgg = sumHistogram(plugin, QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        long queryRequestBytesNoAgg = sumHistogram(plugin, QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        plugin.resetMeter();

        assertSearchHitsWithoutFailures(
            client.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .addAggregation(AggregationBuilders.terms("by_field").field("field")),
            "1",
            "2"
        );
        long queryResultBytesWithAgg = sumHistogram(plugin, QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        long queryRequestBytesWithAgg = sumHistogram(plugin, QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertThat(
            "NodeQueryRequest with aggregation must be larger than without",
            queryRequestBytesWithAgg,
            greaterThan(queryRequestBytesNoAgg)
        );
        assertThat(
            "NodeQueryResponse with aggregation must be larger than without",
            queryResultBytesWithAgg,
            greaterThan(queryResultBytesNoAgg)
        );
    }

    /**
     * Fetch request and result bytes must grow when more hits are requested
     */
    public void testFetchBytesGrowWithSize() {
        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);
        Client coordOnlyNodeClient = client(coordOnlyNode);
        // Fetch the single top hit → one ShardFetchSearchRequest to one shard.
        assertResponse(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()).setSize(1),
            response -> assertFalse("search timed out", response.isTimedOut())
        );
        long fetchRequestBytesSize1 = sumHistogram(plugin, FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long fetchResultBytesSize1 = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        plugin.resetMeter();

        // Fetch both hits → one ShardFetchSearchRequest per shard.
        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()).setSize(10),
            "1",
            "2"
        );
        long fetchRequestBytesSize2 = sumHistogram(plugin, FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long fetchResultBytesSize2 = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);

        assertThat(
            "fetching more hits must accumulate more request bytes than fetching a single hit",
            fetchRequestBytesSize2,
            greaterThan(fetchRequestBytesSize1)
        );
        assertThat(
            "fetching more hits must accumulate more result bytes than fetching a single hit",
            fetchResultBytesSize2,
            greaterThan(fetchResultBytesSize1)
        );
    }

    /**
     * DFS phase bytes must reflect the actual query content:
     * <ul>
     *   <li>DFS result bytes grow with a term query because {@code DfsSearchResult} includes term
     *       statistics (IDF, doc frequencies) that are absent for {@code matchAllQuery}.</li>
     *   <li>DFS query request bytes grow with a term query because {@code QuerySearchRequest} bundles
     *       the {@code DfsSearchResult} (with its term statistics) alongside the shard request.</li>
     *   <li>DFS request bytes grow with a term query because the {@code ShardSearchRequest} must
     *       serialize the additional query fields.</li>
     *   <li>DFS query result bytes grow when an aggregation is present because each shard returns
     *       partial aggregation buckets inside its {@code QuerySearchResult}.</li>
     * </ul>
     */
    public void testDfsBytesReflectQueryAndAggregationContent() {
        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        Client coordOnlyNodeClient = client(coordOnlyNode);
        // Baseline: matchAllQuery produces trivial DFS results (no term statistics).
        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(matchAllQuery()),
            "1",
            "2"
        );
        long dfsResultBaseline = sumHistogram(plugin, DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        long dfsRequestBaseline = sumHistogram(plugin, DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long dfsQueryRequestBaseline = sumHistogram(plugin, DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long dfsQueryResultBaseline = sumHistogram(plugin, DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        plugin.resetMeter();

        // Term query: DfsSearchResult carries term statistics → larger DFS result and query request.
        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(termQuery("field", "value1")),
            "1"
        );
        long dfsResultTerm = sumHistogram(plugin, DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        long dfsRequestTerm = sumHistogram(plugin, DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long dfsQueryRequestTerm = sumHistogram(plugin, DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        plugin.resetMeter();

        assertThat("term query adds term statistics to DfsSearchResult", dfsResultTerm, greaterThan(dfsResultBaseline));
        assertThat("term query serializes more bytes in ShardSearchRequest", dfsRequestTerm, greaterThan(dfsRequestBaseline));
        assertThat(
            "QuerySearchRequest bundles DfsSearchResult so term statistics increase its size",
            dfsQueryRequestTerm,
            greaterThan(dfsQueryRequestBaseline)
        );

        // Aggregation: QuerySearchResult includes partial agg buckets → larger DFS query result.
        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .addAggregation(AggregationBuilders.terms("by_field").field("field")),
            "1",
            "2"
        );

        long dfsRequestWithAgg = sumHistogram(plugin, DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        long dfsQueryResultWithAgg = sumHistogram(plugin, DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
        assertThat("DFS request with aggregation must be larger than without", dfsRequestWithAgg, greaterThan(dfsQueryRequestBaseline));
        assertThat(
            "DFS QuerySearchResult with aggregation must be larger than without",
            dfsQueryResultWithAgg,
            greaterThan(dfsQueryResultBaseline)
        );
    }

    /**
     * Can-match request bytes must reflect the size of the serialized query
     */
    public void testCanMatchRequestBytesReflectQueryComplexity() {
        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        Client coordOnlyNodeClient = client(coordOnlyNode);
        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchAllQuery())
                .addSort(fieldSort("_doc")),
            "1",
            "2"
        );
        long canMatchRequestBytesSimple = sumHistogram(plugin, CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        plugin.resetMeter();

        assertSearchHitsWithoutFailures(
            coordOnlyNodeClient.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(boolQuery().must(matchAllQuery()))
                .addSort(fieldSort("_doc")),
            "1",
            "2"
        );

        long canMatchRequestBytesComplex = sumHistogram(plugin, CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
        assertThat(
            "boolQuery wrapping must serialize more bytes than bare matchAllQuery",
            canMatchRequestBytesComplex,
            greaterThan(canMatchRequestBytesSimple)
        );
    }

    public void testChunkedAndNonChunkedFetchProduceConsistentByteMeasurements() {
        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);

        Client coordOnlyNodeClient = client(coordOnlyNode);
        // Disable chunked fetch: ShardFetchSearchRequest is sent directly without coordinator fields.
        updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), false));
        try {
            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()),
                "1",
                "2"
            );
            long nonChunkedQueryRequestBytes = sumHistogram(plugin, QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            long nonChunkedQueryResultBytes = sumHistogram(plugin, QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            long nonChunkedFetchRequestBytes = sumHistogram(plugin, FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            long nonChunkedFetchResultBytes = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            plugin.resetMeter();

            // Enable chunked fetch: ShardFetchSearchRequest now carries coordinatingNode and coordinatingTaskId.
            updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true));

            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch(INDEX_NAME).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(matchAllQuery()),
                "1",
                "2"
            );
            long chunkedQueryRequestBytes = sumHistogram(plugin, QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            long chunkedQueryResultBytes = sumHistogram(plugin, QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            long chunkedFetchRequestBytes = sumHistogram(plugin, FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            long chunkedFetchResultBytes = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);

            assertThat("query request bytes are fetch-method independent", chunkedQueryRequestBytes, equalTo(nonChunkedQueryRequestBytes));

            // The query result (NodeQueryResponse) includes a per-shard serviceTimeEWMA whose VLong
            // encoding width can shift between runs; empirically this is usually 0 but occasionally a
            // few bytes per shard. Scale with numShards, plus a flat margin for that jitter.
            long queryResultTolerance = numShards * 2L + 24L;
            assertThat(
                "chunked and non-chunked query result bytes are within encoding tolerance",
                chunkedQueryResultBytes,
                both(greaterThanOrEqualTo(nonChunkedQueryResultBytes - queryResultTolerance)).and(
                    lessThanOrEqualTo(nonChunkedQueryResultBytes + queryResultTolerance)
                )
            );

            // Chunked ShardFetchSearchRequest carries extra coordinator fields, making it larger.
            assertThat(
                "chunked ShardFetchSearchRequest includes coordinator fields",
                chunkedFetchRequestBytes,
                greaterThan(nonChunkedFetchRequestBytes)
            );

            // Both paths transfer the same document payload. With all 2 small docs fitting in the
            // last chunk, the only wire difference is a few bytes of format framing (VInt(position)
            // per hit in the chunk format vs the standard SearchHits array encoding). 32 bytes
            // covers this overhead with room to spare.
            long fetchResultTolerance = 32L;
            assertThat(
                "chunked and non-chunked fetch result bytes are within format-overhead tolerance",
                chunkedFetchResultBytes,
                both(greaterThanOrEqualTo(nonChunkedFetchResultBytes - fetchResultTolerance)).and(
                    lessThanOrEqualTo(nonChunkedFetchResultBytes + fetchResultTolerance)
                )
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey()));
        }
    }

    /**
     * Verifies that in the DFS phase, when all remote shards fail, request bytes are recorded
     * (serialisation of the {@code DfsSearchRequest} happens before the error) but result bytes
     * are not.
     */
    public void testDfsPhaseRemoteShardErrorRecordsRequestBytesOnly() {
        final String localIndex = "local-shards-error-test";
        prepareIndex(localIndex).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        ThrowingQueryBuilder failingQuery = new ThrowingQueryBuilder(
            randomLong(),
            new RuntimeException("intentional remote shard failure"),
            INDEX_NAME
        );

        Client coordOnlyNodeClient = client(coordOnlyNode);
        try {
            // use the throwing query, although it does not fail for the index being queried
            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch(localIndex).setQuery(failingQuery).setSearchType(SearchType.DFS_QUERY_THEN_FETCH),
                "1"
            );

            TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);
            List<Measurement> dfsRequestBytes = plugin.getLongHistogramMeasurement(DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertThat(dfsRequestBytes, hasSize(1));
            long dsfRequestBytesLocalIndex = dfsRequestBytes.get(0).getLong();
            assertThat(dsfRequestBytesLocalIndex, greaterThan(0L));
            List<Measurement> dfsResultBytes = plugin.getLongHistogramMeasurement(DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertThat(dfsResultBytes, hasSize(1));
            long dfsResultBytesLocalIndex = dfsResultBytes.get(0).getLong();
            assertThat(dfsResultBytesLocalIndex, greaterThan(0L));

            plugin.resetMeter();

            // localIndex succeeds, so we move past the dfs phase
            assertResponse(
                coordOnlyNodeClient.prepareSearch(INDEX_NAME, localIndex)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(failingQuery)
                    .setAllowPartialSearchResults(true),
                response -> assertThat(response.getFailedShards(), equalTo(numShards))
            );

            dfsRequestBytes = plugin.getLongHistogramMeasurement(DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertThat(dfsRequestBytes, hasSize(1));
            // we hit more shards hence requests use more bytes
            assertThat(dfsRequestBytes.get(0).getLong(), greaterThan(dsfRequestBytesLocalIndex));

            // Error responses are not tracked besides that of localIndex which succeeds
            dfsResultBytes = plugin.getLongHistogramMeasurement(DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertThat(dfsResultBytes, hasSize(1));
            assertEquals(dfsResultBytesLocalIndex, dfsResultBytes.get(0).getLong());

        } finally {
            indicesAdmin().prepareDelete(localIndex).get();
        }
    }

    /**
     * Verifies that in the batched QUERY_THEN_FETCH path, when all remote shards fail, BOTH
     * request bytes and result bytes are recorded. In the batched path the coordinator sends a
     * single {@code NodeQueryRequest} to the data node and receives a {@code NodeQueryResponse}
     * that carries per-shard exceptions as payload rather than as a transport-level error.
     */
    public void testBatchedQueryPhaseShardErrorRecordsBothRequestAndResultBytes() {
        // we use this local index to ensure that we go past the query phase
        final String localIndex = "local-shards-error-test";
        // At least 2 shards per data node so both searches below are always batched.
        int localIndexShards = between(2 * internalCluster().numDataNodes(), maximumNumberOfShards());
        prepareCreate(localIndex).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, localIndexShards)).get();
        ensureGreen(localIndex);
        prepareIndex(localIndex).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        try {
            ThrowingQueryBuilder failingQuery = new ThrowingQueryBuilder(
                randomLong(),
                new RuntimeException("intentional remote shard failure"),
                INDEX_NAME
            );

            // Shards on a node share one deserialized failingQuery; doWriteTo always serializes
            // its failure field regardless of whether the shard throws. Without error_trace=true,
            // SearchService strips the stack trace in place when INDEX_NAME shards throw it, so
            // localIndex shards echo a stripped exception — fewer bytes despite more shards.
            Client coordOnlyNodeClient = client(coordOnlyNode).filterWithHeader(Map.of("error_trace", "true"));
            // query only the local index and record the baseline for result bytes
            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch(localIndex).setQuery(failingQuery).setAllowPartialSearchResults(true),
                "1"
            );

            TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);
            List<Measurement> queryRequestBytes = plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertThat(queryRequestBytes, hasSize(1));
            long queryRequestBytesLocalIndex = queryRequestBytes.get(0).getLong();
            assertThat(queryRequestBytesLocalIndex, greaterThan(0L));
            List<Measurement> queryResultBytes = plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertThat(queryResultBytes, hasSize(1));
            long queryResultBytesLocalIndex = queryResultBytes.get(0).getLong();
            assertThat(queryResultBytesLocalIndex, greaterThan(0L));

            plugin.resetMeter();

            assertResponse(
                coordOnlyNodeClient.prepareSearch(INDEX_NAME, localIndex).setQuery(failingQuery).setAllowPartialSearchResults(true),
                response -> assertThat(response.getFailedShards(), equalTo(numShards))
            );

            // NodeQueryRequest was serialised before the shards returned errors.
            queryRequestBytes = plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME);
            assertThat(queryRequestBytes, hasSize(1));
            assertThat(queryRequestBytes.get(0).getLong(), greaterThan(queryRequestBytesLocalIndex));

            // Both searches are batched, so their result bytes are comparable: batched failures are folded
            // into the merged NodeQueryResponse as compact exception stamps, not full QuerySearchResult
            // objects, so batched and non-batched byte counts sit on different scales.
            queryResultBytes = plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            assertThat(queryResultBytes, hasSize(1));
            assertThat(queryResultBytes.get(0).getLong(), greaterThan(queryResultBytesLocalIndex));
        } finally {
            indicesAdmin().prepareDelete(localIndex).get();
        }
    }

    /**
     * Verifies that when every shard fails, neither request bytes nor result bytes are recorded.
     */
    public void testAllShardsFailedRecordsNoBytes() {
        ThrowingQueryBuilder failingQuery = new ThrowingQueryBuilder(randomLong(), new RuntimeException("intentional failure"), INDEX_NAME);

        expectThrows(
            SearchPhaseExecutionException.class,
            () -> client(coordOnlyNode).prepareSearch(INDEX_NAME).setQuery(failingQuery).get()
        );

        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);
        assertThat(plugin.getLongHistogramMeasurement(QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME), empty());
        assertThat(plugin.getLongHistogramMeasurement(QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME), empty());
    }

    /**
     * Verifies that intermediate {@link org.elasticsearch.transport.BytesTransportRequest} chunks
     * sent from the data node during chunked fetch are included in the fetch-result histogram.
     *
     * <p>With the minimum chunk threshold (256 KB), two documents whose serialized source exceeds
     * 256 KB each force the streaming fetch to send an intermediate chunk for the first document
     * before the second document goes into the final {@link org.elasticsearch.search.fetch.FetchSearchResult}.
     */
    public void testChunkedFetchCountsIntermediateChunkBytes() {
        // Use a single shard index so all docs are co-located for this test
        // Add a field that is stored in _source but not indexed, so we can store large values
        // without hitting Lucene's term-size limit or keyword indexing restrictions.
        prepareCreate("chunked_test_index").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .setMapping("{\"properties\":{\"bigdata\":{\"type\":\"keyword\",\"index\":false,\"doc_values\":false}}}")
            .get();

        // Each document source is ~300 KB. With the 256 KB chunk threshold, the first document
        // fills an intermediate BytesTransportRequest chunk; the second becomes the last chunk
        // embedded in FetchSearchResult.
        String bigdata = "x".repeat(300_000);
        prepareIndex("chunked_test_index").setId("large1")
            .setSource("field", "large_doc", "bigdata", bigdata)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("chunked_test_index").setId("large2")
            .setSource("field", "large_doc", "bigdata", bigdata)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        TestTelemetryPlugin plugin = getPlugin(coordOnlyNode);
        try {
            Client coordOnlyNodeClient = client(coordOnlyNode);
            updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_TARGET_CHUNK_BYTES.getKey(), "256kb"));
            // Non-chunked baseline: a single FetchSearchResult carries both documents.
            updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), false));
            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch("chunked_test_index").setQuery(termQuery("field", "large_doc")),
                "large1",
                "large2"
            );
            long nonChunkedFetchResultBytes = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);
            plugin.resetMeter();

            // Chunked path: an intermediate chunk carries doc1 and the FetchSearchResult carries doc2.
            // Both must be counted so the total approximates the non-chunked total.
            updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true));
            assertSearchHitsWithoutFailures(
                coordOnlyNodeClient.prepareSearch("chunked_test_index").setQuery(termQuery("field", "large_doc")),
                "large1",
                "large2"
            );
            long chunkedFetchResultBytes = sumHistogram(plugin, FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME);

            // The tolerance covers per-chunk format overhead (routing VLong, shardId, hit-position
            // VInts) minus differences in SearchHits vs chunk-stream framing. For 1 intermediate
            // chunk the overhead is well under 512 bytes.
            long fetchResultTolerance = 512L;
            assertThat(
                "chunked fetch with intermediate chunks must count all bytes (not just the last chunk)",
                chunkedFetchResultBytes,
                both(greaterThanOrEqualTo(nonChunkedFetchResultBytes - fetchResultTolerance)).and(
                    lessThanOrEqualTo(nonChunkedFetchResultBytes + fetchResultTolerance)
                )
            );
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey())
                    .putNull(SearchService.FETCH_PHASE_CHUNKED_TARGET_CHUNK_BYTES.getKey())
            );
            indicesAdmin().prepareDelete("chunked_test_index").get();
        }
    }
}
