/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.msearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MultiSearchIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
    }

    public void testSimpleMultiSearch() {
        createIndex("test");
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "xxx").get();
        prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())),
            response -> {
                for (Item item : response) {
                    assertNoFailures(item.getResponse());
                }
                assertThat(response.getResponses().length, equalTo(3));
                assertHitCount(response.getResponses()[0].getResponse(), 1L);
                assertHitCount(response.getResponses()[1].getResponse(), 1L);
                assertHitCount(response.getResponses()[2].getResponse(), 2L);
                assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
                assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
            }
        );
    }

    /**
     * Verifies that {@code TransportMultiSearchAction} accounts for buffered sub-search responses on the
     * coordinating node's {@link CircuitBreaker#REQUEST} breaker while the msearch is in flight, and releases
     * the reservation when the combined {@link org.elasticsearch.action.search.MultiSearchResponse} is delivered.
     */
    public void testBreakerAccountingEndToEnd() throws Exception {
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        // The request breaker type is randomized per node (~10% noop). The coordinating-only node gets its own seed and may
        // be noop even when the pre-existing nodes are not, so the check must target the coordinator that runs the msearch
        // accounting rather than the pre-existing nodes; a noop breaker never reserves bytes and reports getUsed() == 0.
        assumeFalse("coordinator uses a noop request breaker, skipping test", noopBreakerUsed(coordinatorNode));

        String index = "msearch-breaker-it";
        int numDocs = scaledRandomIntBetween(20, 50);
        int numSearches = scaledRandomIntBetween(10, 25);

        createIndex(index);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            indexRequests.add(
                prepareIndex(index).setId(Integer.toString(i)).setSource("payload", "x".repeat(scaledRandomIntBetween(1_000, 5_000)))
            );
        }
        indexRandom(true, indexRequests);
        ensureGreen(index);

        assertBusy(
            () -> assertThat(
                "request breaker should be at baseline before msearch",
                requestBreakerEstimated(coordinatorNode),
                lessThanOrEqualTo(0L)
            )
        );
        long baseline = requestBreakerEstimated(coordinatorNode);

        // Polls every 1 ms; with maxConcurrentSearchRequests(1) and many sub-searches the reservation
        // stays elevated long enough to observe. A sub-millisecond msearch could theoretically complete
        // between polls and leave peakUsage at baseline.
        AtomicLong peakUsage = new AtomicLong(baseline);
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "msearch-breaker-monitor");
            t.setDaemon(true);
            return t;
        });
        monitor.scheduleWithFixedDelay(() -> {
            long used = requestBreakerEstimated(coordinatorNode);
            peakUsage.updateAndGet(current -> Math.max(current, used));
        }, 0, 1, TimeUnit.MILLISECONDS);

        try {
            MultiSearchRequest request = new MultiSearchRequest();
            request.maxConcurrentSearchRequests(1);
            var coordinatorClient = internalCluster().client(coordinatorNode);
            for (int i = 0; i < numSearches; i++) {
                request.add(coordinatorClient.prepareSearch(index).setSize(numDocs).setQuery(QueryBuilders.matchAllQuery()).request());
            }
            assertResponse(coordinatorClient.multiSearch(request), response -> {
                assertThat(response.getResponses().length, equalTo(numSearches));
                for (Item item : response) {
                    assertNoFailures(item.getResponse());
                }
            });
        } finally {
            monitor.shutdownNow();
            assertTrue(monitor.awaitTermination(30, TimeUnit.SECONDS));
        }

        assertThat(
            "msearch should reserve request breaker bytes on the coordinator while sub-responses are buffered",
            peakUsage.get(),
            greaterThan(baseline)
        );
        assertBusy(
            () -> assertThat(
                "request breaker should return to baseline after msearch completes",
                requestBreakerEstimated(coordinatorNode),
                lessThanOrEqualTo(baseline)
            )
        );
    }

    public void testSimpleMultiSearchMoreRequests() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(0, 16);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        refresh();

        int numSearchRequests = randomIntBetween(1, 64);
        MultiSearchRequest request = new MultiSearchRequest();
        if (randomBoolean()) {
            request.maxConcurrentSearchRequests(randomIntBetween(1, numSearchRequests));
        }
        for (int i = 0; i < numSearchRequests; i++) {
            request.add(prepareSearch("test"));
        }
        assertResponse(client().multiSearch(request), response -> {
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            for (Item item : response) {
                assertNoFailures(item.getResponse());
                assertHitCount(item.getResponse(), numDocs);
            }
        });
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before
     * TransportVersion.minimumCCSVersion() works
     */
    public void testCCSCheckCompatibility() throws Exception {
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersion.minimumCCSVersion(), true);
        createIndex("test");
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "xxx").get();
        prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(prepareSearch("test").setQuery(new DummyQueryBuilder() {
                    @Override
                    public TransportVersion getMinimalSupportedVersion() {
                        return transportVersion;
                    }
                })),
            response -> {
                assertThat(response.getResponses().length, equalTo(3));
                assertHitCount(response.getResponses()[0].getResponse(), 1L);
                assertHitCount(response.getResponses()[1].getResponse(), 1L);
                assertTrue(response.getResponses()[2].isFailure());
                assertTrue(
                    response.getResponses()[2].getFailure().getMessage().contains("the 'search.check_ccs_compatibility' setting is enabled")
                );
            }
        );
    }

    public void testMrtValuesArePickedCorrectly() throws IOException {

        {
            // If no MRT is specified, all searches should default to true.
            String body = """
                {"index": "index-1" }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of());
            for (SearchRequest req : mreq.requests()) {
                assertTrue(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            // MRT query param is false, so all searches should use this value.
            String body = """
                {"index": "index-1" }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of("ccs_minimize_roundtrips", "false"));
            for (SearchRequest req : mreq.requests()) {
                assertFalse(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            // Query param is absent but MRT is specified for each request.
            String body = """
                {"index": "index-1", "ccs_minimize_roundtrips": false }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2", "ccs_minimize_roundtrips": false }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of());
            for (SearchRequest req : mreq.requests()) {
                assertFalse(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            /*
             * The first request overrides the query param and should use MRT=true.
             * The second request should use the query param value.
             */
            String body = """
                {"index": "index-1", "ccs_minimize_roundtrips": true }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of("ccs_minimize_roundtrips", "false"));

            assertThat(mreq.requests().size(), Matchers.is(2));
            assertTrue(mreq.requests().getFirst().isCcsMinimizeRoundtrips());
            assertFalse(mreq.requests().getLast().isCcsMinimizeRoundtrips());
        }
    }

    public void testTopLevelSliceParamIsAppliedToAllSubRequests() throws IOException {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String body = """
            {"index": "index-1" }
            {"query" : {"match" : { "message": "this is a test"}}}
            {"index": "index-2" }
            {"query" : {"match_all" : {}}}
            """;
        MultiSearchRequest mreq = parseRequest(body, Map.of(SliceIndexing.PARAM_NAME, "s1,s2"));
        assertThat(mreq.requests().size(), Matchers.is(2));
        for (SearchRequest req : mreq.requests()) {
            assertEquals("s1,s2", req.routing());
            assertTrue(req.isRoutingFromSlice());
            assertEquals("s1,s2", req.searchSlice());
        }
    }

    public void testRoutingAndSliceCannotBeMixedAcrossRequestLevels() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String body = """
            {"_slice": "s1" }
            {"query" : {"match_all" : {}}}
            """;
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseRequest(body, Map.of("routing", "r1")));
        assertThat(ex.getMessage(), Matchers.is("[routing] and [_slice] cannot be combined in the same _msearch request"));
    }

    public void testSliceEnabledIndexDefaultsToAllAndRejectsRoutingInExecution() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("slice-enabled", Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 1).build());
        ensureGreen("slice-enabled");

        MultiSearchRequest request = new MultiSearchRequest();
        request.add(
            new SearchRequest("slice-enabled").source(
                new org.elasticsearch.search.builder.SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            )
        );
        request.add(
            new SearchRequest("slice-enabled").routing("r1")
                .source(new org.elasticsearch.search.builder.SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))
        );
        assertResponse(client().multiSearch(request), response -> {
            assertThat(response.getResponses().length, equalTo(2));
            assertFalse(response.getResponses()[0].isFailure());
            assertTrue(response.getResponses()[1].isFailure());
            assertThat(
                response.getResponses()[1].getFailure().getMessage(),
                containsString("[routing] is not allowed when [index.slice.enabled] is true")
            );
            assertThat(response.getResponses()[1].getFailure().getMessage(), containsString("use [_slice] instead"));
        });
    }

    public void testExecutionWithMultipleRoutingAndSliceMetadataOptions() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("routing-index", Settings.builder().put("number_of_shards", 1).build());
        createIndex("slice-index", Settings.builder().put("index.slice.enabled", true).put("number_of_shards", 1).build());
        ensureGreen("routing-index", "slice-index");

        prepareIndex("routing-index").setId("r1-doc").setRouting("r1").setSource("field", "routing-r1").get();
        prepareIndex("routing-index").setId("r2-doc").setRouting("r2").setSource("field", "routing-r2").get();
        client().index(new IndexRequest("slice-index").id("s1-doc").routing("s1").setRoutingFromSlice(true).source("field", "slice-s1"))
            .actionGet();
        client().index(new IndexRequest("slice-index").id("s2-doc").routing("s2").setRoutingFromSlice(true).source("field", "slice-s2"))
            .actionGet();
        refresh();

        String body = """
            {"index":"routing-index","routing":"r1"}
            {"query":{"term":{"field.keyword":"routing-r1"}}}
            {"index":"slice-index","_slice":"s1"}
            {"query":{"term":{"field.keyword":"slice-s1"}}}
            {"index":"routing-index","routing":"r2"}
            {"query":{"term":{"field.keyword":"routing-r2"}}}
            {"index":"slice-index","_slice":"s2"}
            {"query":{"term":{"field.keyword":"slice-s2"}}}
            """;
        MultiSearchRequest request = parseRequest(body, Map.of());
        assertThat(request.requests().size(), equalTo(4));
        assertEquals("r1", request.requests().get(0).routing());
        assertFalse(request.requests().get(0).isRoutingFromSlice());
        assertNull(request.requests().get(0).searchSlice());
        assertEquals("s1", request.requests().get(1).routing());
        assertTrue(request.requests().get(1).isRoutingFromSlice());
        assertEquals("s1", request.requests().get(1).searchSlice());
        assertEquals("r2", request.requests().get(2).routing());
        assertFalse(request.requests().get(2).isRoutingFromSlice());
        assertNull(request.requests().get(2).searchSlice());
        assertEquals("s2", request.requests().get(3).routing());
        assertTrue(request.requests().get(3).isRoutingFromSlice());
        assertEquals("s2", request.requests().get(3).searchSlice());

        assertResponse(client().multiSearch(request), response -> {
            assertThat(response.getResponses().length, equalTo(4));
            for (Item item : response) {
                assertNoFailures(item.getResponse());
                assertHitCount(item.getResponse(), 1L);
            }
            assertFirstHit(response.getResponses()[0].getResponse(), hasId("r1-doc"));
            assertFirstHit(response.getResponses()[1].getResponse(), hasId("s1-doc"));
            assertFirstHit(response.getResponses()[2].getResponse(), hasId("r2-doc"));
            assertFirstHit(response.getResponses()[3].getResponse(), hasId("s2-doc"));
        });
    }

    private RestRequest mkRequest(String body, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/index*/_msearch")
            .withParams(params)
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }

    private MultiSearchRequest parseRequest(String body, Map<String, String> params) throws IOException {
        return RestMultiSearchAction.parseRequest(
            mkRequest(body, params),
            true,
            new UsageService().getSearchUsageHolder(),
            (ignored) -> true,
            // Disable CPS for these tests.
            Optional.of(false)
        );
    }

    private boolean noopBreakerUsed(String nodeName) {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats(nodeName).setBreaker(true).get();
        for (NodeStats nodeStats : stats.getNodes()) {
            if (nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
        }
        return false;
    }

    private long requestBreakerEstimated(String nodeName) {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats(nodeName).setBreaker(true).get();
        long estimated = 0;
        for (NodeStats nodeStats : stats.getNodes()) {
            CircuitBreakerStats breakerStats = nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST);
            if (breakerStats != null) {
                estimated += breakerStats.getEstimated();
            }
        }
        return estimated;
    }
}
