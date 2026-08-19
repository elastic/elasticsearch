/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.search.SearchService.FETCH_PHASE_CHUNKED_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

public class BytesReadHeaderIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // registers the ThrowingQueryBuilder used to simulate shard failures
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestQueryBuilderPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        // enable http so the HTTP-level single-header tests can assert the consolidated response header
        return false;
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            }));
        }
    }

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testEmptySearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, 0);
        SearchRequest request = new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, equalTo(0L));
    }

    public void testPointInTimeSearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, 100);

        BytesReference pitId = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet().getPointInTimeId();
        try {
            SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                .pointInTimeBuilder(new PointInTimeBuilder(pitId));
            long bytesRead = assertBytesReadHeader(new SearchRequest().source(source));
            assertThat(bytesRead, greaterThan(0L));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testBiggerIndexReadsMoreBytes() throws InterruptedException {
        int numDocs = atLeast(200);

        String smallerIndex = randomIndexName();
        String biggerIndex = randomIndexName();

        createIndex(smallerIndex, 1, 0);
        createIndex(biggerIndex, 1, 0);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String data = "value value" + i;
            builders.add(prepareIndex(smallerIndex).setSource("field", data));
            builders.add(prepareIndex(biggerIndex).setSource("field", data));
            builders.add(prepareIndex(biggerIndex).setSource("field", data + " " + i));
        }

        indexRandom(true, false, false, builders);
        flushAndRefresh(smallerIndex, biggerIndex);
        forceMerge(true);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));
        long smallerBytesRead = assertBytesReadHeader(new SearchRequest(smallerIndex).source(source));
        long biggerBytesRead = assertBytesReadHeader(new SearchRequest(biggerIndex).source(source));

        assertThat(smallerBytesRead, greaterThan(0L));
        assertThat(biggerBytesRead, greaterThan(smallerBytesRead));
    }

    public void testRequestCacheWorks() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .size(0)
            .aggregation(AggregationBuilders.terms("agg").field("field.keyword"));

        // initial request fills the cache
        long initialBytesRead = assertBytesReadHeader(new SearchRequest(indexName).source(source).requestCache(true));
        assertThat(initialBytesRead, greaterThan(0L));

        long cachedBytesRead = assertBytesReadHeader(new SearchRequest(indexName).source(source).requestCache(true));
        assertThat(cachedBytesRead, equalTo(0L));
    }

    public void testDfsQueryThenFetchReadsMoreDataThanQueryThenFetch() throws InterruptedException {
        final String indexName = setupIndex(2, atLeast(200));
        forceMerge(true);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));

        long queryThenFetchBytesRead = assertBytesReadHeader(
            new SearchRequest(indexName).source(source).searchType(SearchType.QUERY_THEN_FETCH)
        );

        long dfsQueryThenFetchBytesRead = assertBytesReadHeader(
            new SearchRequest(indexName).source(source).searchType(SearchType.DFS_QUERY_THEN_FETCH)
        );

        assertThat(queryThenFetchBytesRead, greaterThan(0L));
        assertThat(dfsQueryThenFetchBytesRead, greaterThan(0L));
        assertThat(dfsQueryThenFetchBytesRead, greaterThan(queryThenFetchBytesRead));
    }

    public void testLargerResultSizeReportsMoreBytesRead() throws InterruptedException {
        final String indexName = setupIndex(1, atLeast(200));

        SearchSourceBuilder baseSource = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("f", SortOrder.ASC);

        // warmup
        assertBytesReadHeader(new SearchRequest(indexName).source(baseSource));

        SearchRequest largeRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("f", SortOrder.ASC).size(100)
        );
        long largeBytesRead = assertBytesReadHeader(largeRequest);

        SearchRequest smallRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("f", SortOrder.ASC).size(1)
        );
        long smallBytesRead = assertBytesReadHeader(smallRequest);

        assertThat(largeBytesRead, greaterThan(smallBytesRead));
    }

    public void testScrollSearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, 100);
        SearchRequest request = new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1))
            .scroll(TimeValue.timeValueMinutes(1));

        SetOnce<String> scrollId = new SetOnce<>();
        final Client client = client();
        assertBytesReadHeader(request, searchResponse -> scrollId.set(searchResponse.getScrollId()));
        assertThat(scrollId.get(), notNullValue());

        try {
            SetOnce<Long> scrollBytesRead = new SetOnce<>();
            CountDownLatch scrollLatch = new CountDownLatch(1);

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId.get()).scroll(TimeValue.timeValueMinutes(1));
            client.searchScroll(
                scrollRequest,
                new LatchedActionListener<>(
                    ActionListener.assertOnce(
                        ActionListener.wrap(
                            searchResponse -> scrollBytesRead.set(storeBytesRead(searchResponse)),
                            e -> fail("no error expected")
                        )
                    ),
                    scrollLatch
                )
            );

            assertTrue("scroll search did not complete in time", scrollLatch.await(30, TimeUnit.SECONDS));
            assertThat(scrollBytesRead.get(), greaterThan(0L));
        } finally {
            client.prepareClearScroll().addScrollId(scrollId.get()).get();
        }
    }

    public void testCountOnlySearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, atLeast(50));
        // size=0 + track_total_hits routes through CountOnlyQueryPhaseResultConsumer; a term query forces reading
        // postings on the data node so metrics should be non-zero.
        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(0).trackTotalHits(true)
        );
        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testDfsQueryThenFetchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = randomIndexName();
        createIndex(indexName, between(2, 5), 0);
        IntStream.range(0, between(10, 50)).forEach(i -> indexDoc(indexName, "id" + i, "field", "value" + i));
        flushAndRefresh(indexName);

        SearchRequest request = new SearchRequest(indexName).searchType(SearchType.DFS_QUERY_THEN_FETCH)
            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testChunkedFetching() throws InterruptedException {
        final String indexName = setupIndex(between(2, 5), 0);
        // each document has at least 10kb of source
        IntStream.range(0, between(100, 200))
            .forEach(i -> indexDoc(indexName, "id" + i, "field", "value value" + i + " " + randomAlphaOfLength(10000)));
        flushAndRefresh(indexName);

        // warmup
        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(50)
        );

        // it should not matter if chunked fetching is enabled or disabled, the number of bytes should remain the same
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setPersistentSettings(Settings.builder().put(FETCH_PHASE_CHUNKED_ENABLED.getKey(), true))
        );

        long bytesReadChunkedFetch = assertBytesReadHeader(request);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setPersistentSettings(Settings.builder().putNull(FETCH_PHASE_CHUNKED_ENABLED.getKey()))
                .get()
        );

        long bytesRead = assertBytesReadHeader(request);

        assertThat(bytesReadChunkedFetch, greaterThan(0L));
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testPartialShardFailureStillReportsBytesRead() throws InterruptedException {
        final String indexName = setupIndex(between(3, 5), atLeast(200));

        // throw only on shard 0; the remaining shards execute normally and read from their directories
        ThrowingQueryBuilder query = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("simulated shard failure"), 0);
        SearchRequest request = new SearchRequest(indexName).allowPartialSearchResults(true)
            .source(new SearchSourceBuilder().query(query).size(50));

        SetOnce<Integer> failedShards = new SetOnce<>();
        long bytesRead = assertBytesReadHeader(request, response -> failedShards.set(response.getFailedShards()));

        assertThat("expected at least one failed shard", failedShards.get(), greaterThan(0));
        // the surviving shards still read from their directories, so the accumulated header must be non-zero
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testMultipleIndicesAccumulateBytesRead() throws InterruptedException {
        int numDocs = between(100, 300);
        final String firstIndex = setupIndex(between(2, 4), numDocs);
        final String secondIndex = setupIndex(between(2, 4), numDocs);

        assertNoFailures(indicesAdmin().prepareForceMerge(firstIndex, secondIndex).setMaxNumSegments(1).get());

        int fetchAll = numDocs * 2;
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(fetchAll);

        // close+reopen before each search so every query reads from a cold store (no cached readers)
        long firstBytesRead = assertColdBytesRead(new SearchRequest(firstIndex).source(source), firstIndex);
        long secondBytesRead = assertColdBytesRead(new SearchRequest(secondIndex).source(source), secondIndex);
        long combinedBytesRead = assertColdBytesRead(new SearchRequest(firstIndex, secondIndex).source(source), firstIndex, secondIndex);

        assertThat(firstBytesRead, greaterThan(0L));
        assertThat(secondBytesRead, greaterThan(0L));
        assertThat(combinedBytesRead, greaterThanOrEqualTo(firstBytesRead + secondBytesRead));
    }

    private long assertColdBytesRead(SearchRequest request, String... indices) throws InterruptedException {
        assertAcked(indicesAdmin().prepareClose(indices));
        assertAcked(indicesAdmin().prepareOpen(indices));
        ensureGreen(indices);
        return assertBytesReadHeader(request);
    }

    public void testFullShardFailureDoesNotSetBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(between(2, 5), atLeast(50));

        ThrowingQueryBuilder query = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("simulated shard failure"), -1);
        SearchRequest request = new SearchRequest(indexName).allowPartialSearchResults(true)
            .source(new SearchSourceBuilder().query(query).size(10));

        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<Exception> failure = new SetOnce<>();
        client().search(request, new LatchedActionListener<>(ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    fail("expected the search to fail when all shards fail");
                } finally {
                    searchResponse.decRef();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        }), latch));

        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        assertThat(failure.get(), notNullValue());
    }

    public void testZeroMatchingDocumentsSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, atLeast(50));

        // a term that does not exist in the index: the query matches nothing but the header must still be emitted
        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "no_such_term"))
        );
        SetOnce<Long> totalHits = new SetOnce<>();
        long bytesRead = assertBytesReadHeader(request, response -> totalHits.set(response.getHits().getTotalHits().value()));

        assertThat(totalHits.get(), equalTo(0L));
        assertThat(bytesRead, greaterThanOrEqualTo(0L));
    }

    public void testAggregationOnlySearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, atLeast(200));

        // size=0 aggregation reads doc values (global ordinals) on the data node, without running a fetch phase
        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("agg").field("field.keyword"))
        );
        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testStoredFieldsFetchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 0)).setMapping("field", "type=text,store=true"));
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i));
        }
        indexRandom(true, false, false, builders);
        flushAndRefresh(indexName);

        // fetching a stored field forces reads of the stored-fields files during the fetch phase
        SearchRequest storedFieldsRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).storedField("field").fetchSource(false).size(50)
        );
        long storedFieldsBytesRead = assertBytesReadHeader(storedFieldsRequest);

        // a count-only request over the same data performs no fetch phase, so it reads fewer bytes
        SearchRequest countRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(0).trackTotalHits(true)
        );
        long countBytesRead = assertBytesReadHeader(countRequest);

        assertThat(storedFieldsBytesRead, greaterThan(0L));
        assertThat(storedFieldsBytesRead, greaterThan(countBytesRead));
    }

    public void testTerminateAfterReadsFewerBytes() throws InterruptedException {
        int numDocs = atLeast(500);
        final String indexName = setupIndex(1, numDocs);

        SearchRequest fullRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(numDocs)
        );
        long fullBytesRead = assertBytesReadHeader(fullRequest);

        SearchRequest terminatedRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(numDocs).terminateAfter(1)
        );
        SetOnce<Boolean> terminatedEarly = new SetOnce<>();
        long terminatedBytesRead = assertBytesReadHeader(terminatedRequest, response -> terminatedEarly.set(response.isTerminatedEarly()));

        assertThat(terminatedEarly.get(), equalTo(true));
        assertThat(terminatedBytesRead, greaterThan(0L));
        // terminating after a single document collects and fetches far less data, so fewer bytes are read
        assertThat(terminatedBytesRead, lessThan(fullBytesRead));
    }

    public void testSearchAfterSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex(1, atLeast(200));

        SearchRequest firstPage = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("f", SortOrder.ASC).size(10)
        );
        SetOnce<Object[]> lastSortValues = new SetOnce<>();
        long firstPageBytesRead = assertBytesReadHeader(firstPage, response -> {
            SearchHit[] hits = response.getHits().getHits();
            assertThat(hits.length, greaterThan(0));
            lastSortValues.set(hits[hits.length - 1].getSortValues());
        });

        SearchRequest secondPage = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
                .sort("f", SortOrder.ASC)
                .searchAfter(lastSortValues.get())
                .size(10)
        );
        long secondPageBytesRead = assertBytesReadHeader(secondPage);

        assertThat(firstPageBytesRead, greaterThan(0L));
        assertThat(secondPageBytesRead, greaterThan(0L));
    }

    public void testMultiSearchSetsBytesReadHeader() throws InterruptedException {
        int numDocs = atLeast(100);
        final String firstIndex = setupIndex(1, numDocs);
        final String secondIndex = setupIndex(1, numDocs);

        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest(firstIndex).source(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))));
        request.add(new SearchRequest(secondIndex).source(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))));

        SetOnce<Long> totalBytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().multiSearch(request, new LatchedActionListener<>(ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(MultiSearchResponse response) {
                for (MultiSearchResponse.Item item : response.getResponses()) {
                    assertThat("no sub-search failure expected: " + item.getFailureMessage(), item.isFailure(), equalTo(false));
                }
                totalBytesRead.set(storeBytesRead(response.mergeDirectoryMetrics()));
            }

            @Override
            public void onFailure(Exception e) {
                fail("no error expected");
            }
        }), latch));

        assertTrue("multi search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        assertThat("the multi-search response exposes a single consolidated metrics total", totalBytesRead.get(), greaterThan(0L));
    }

    public void testSearchHttpResponseHasSingleMetricsHeader() throws Exception {
        final String indexName = setupIndex(between(2, 4), atLeast(100));
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("{\"query\":{\"term\":{\"field\":\"value\"}}}");
        Response response = getRestClient().performRequest(request);
        assertSingleMetricsHeader(response);
    }

    public void testMultiSearchHttpResponseHasSingleMetricsHeader() throws Exception {
        final String firstIndex = setupIndex(1, atLeast(50));
        final String secondIndex = setupIndex(1, atLeast(50));
        Request request = new Request("POST", "/_msearch");

        request.setJsonEntity(
            "{\"index\":\""
                + firstIndex
                + "\"}\n{\"query\":{\"term\":{\"field\":\"value\"}}}\n"
                + "{\"index\":\""
                + secondIndex
                + "\"}\n{\"query\":{\"term\":{\"field\":\"value\"}}}\n"
        );
        Response response = getRestClient().performRequest(request);
        assertSingleMetricsHeader(response);
    }

    private static void assertSingleMetricsHeader(Response response) {
        long headerCount = Arrays.stream(response.getHeaders())
            .filter(header -> header.getName().equalsIgnoreCase(RestActions.SEARCH_METRICS_RESPONSE_HEADER))
            .count();
        assertThat("expected exactly one consolidated metrics header", headerCount, equalTo(1L));
    }

    private String setupIndex(int primaryShards, int documents) {
        final String indexName = randomIndexName();
        createIndex(indexName, primaryShards, 0);
        IntStream.range(0, documents).forEach(i -> indexDoc(indexName, "id " + i, "f", i, "field", "value value" + i));
        flushAndRefresh(indexName);
        return indexName;
    }

    public static long assertBytesReadHeader(SearchRequest searchRequest) throws InterruptedException {
        return assertBytesReadHeader(searchRequest, null);
    }

    public static long assertBytesReadHeader(SearchRequest searchRequest, Consumer<SearchResponse> searchResponseConsumer)
        throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        SetOnce<Exception> failure = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        client().search(searchRequest, new LatchedActionListener<>(ActionListener.assertOnce(ActionListener.wrap(searchResponse -> {
            bytesRead.set(storeBytesRead(searchResponse));
            if (searchResponseConsumer != null) {
                searchResponseConsumer.accept(searchResponse);
            }
        }, failure::set)), latch));
        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("unexpected search failure", failure.get());
        }
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }

    public static long storeBytesRead(SearchResponse response) {
        return storeBytesRead(response.getDirectoryMetrics());
    }

    public static long storeBytesRead(DirectoryMetrics metrics) {
        String value = metrics.entries().get(StoreMetrics.BYTES_READ_METRIC_KEY);
        return value == null ? 0L : Long.parseLong(value);
    }
}
