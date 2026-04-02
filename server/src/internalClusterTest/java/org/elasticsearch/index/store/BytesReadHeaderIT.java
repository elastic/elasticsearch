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
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.search.SearchService.FETCH_PHASE_CHUNKED_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests that the {@code X-Elasticsearch-Bytes-Read} response header is set on data nodes during search
 * and propagated to the coordinating node across different search types.
 */
public class BytesReadHeaderIT extends ESIntegTestCase {

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testSearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex();
        SearchRequest request = new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testPointInTimeSearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex();

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

    public void testLargerResultSizeReportsMoreBytesRead() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder baseSource = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);

        // warmup
        assertBytesReadHeader(new SearchRequest(indexName).source(baseSource));

        SearchRequest largeRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("id", SortOrder.ASC).size(100)
        );
        long largeBytesRead = assertBytesReadHeader(largeRequest);

        SearchRequest smallRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("id", SortOrder.ASC).size(1)
        );
        long smallBytesRead = assertBytesReadHeader(smallRequest);

        assertThat(largeBytesRead, greaterThan(smallBytesRead));
    }

    public void testScrollSearchSetsBytesReadHeader() throws InterruptedException {
        final String indexName = setupIndex();
        SearchRequest request = new SearchRequest(indexName).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(1))
            .scroll(TimeValue.timeValueMinutes(1));

        SetOnce<String> scrollId = new SetOnce<>();
        SetOnce<Long> initialBytesRead = new SetOnce<>();
        CountDownLatch initialLatch = new CountDownLatch(1);

        final Client client = client();
        assertBytesReadHeader(request, searchResponse -> scrollId.set(searchResponse.getScrollId()));

        assertTrue("initial search did not complete in time", initialLatch.await(30, TimeUnit.SECONDS));
        assertThat(initialBytesRead.get(), greaterThan(0L));
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
                            searchResponse -> scrollBytesRead.set(extractBytesReadHeader(client)),
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
        final String indexName = randomIndexName();
        createIndex(indexName, between(2, 5), 0);
        // each document has at least 10kb of source
        IntStream.range(0, between(100, 200))
            .forEach(i -> indexDoc(indexName, "id" + i, "field", "value value" + i + " " + randomAlphaOfLength(10000)));
        flushAndRefresh(indexName);

        // warmup
        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).size(50)
        );

        // it should not matter if chunked fetching is enabled or disabled, the number of bytes should remain the same
        assertAcked(client().admin().cluster().prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
            .setPersistentSettings(Settings.builder().put(FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)));

        long bytesReadChunkedFetch = assertBytesReadHeader(request);

        assertAcked(client().admin().cluster().prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
            .setPersistentSettings(Settings.builder().putNull(FETCH_PHASE_CHUNKED_ENABLED.getKey())).get());

        long bytesRead = assertBytesReadHeader(request);

        assertThat(bytesRead, greaterThan(0L));
        assertThat(bytesRead, equalTo(bytesReadChunkedFetch));
    }

    private String setupIndex() {
        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        IntStream.range(0, between(10, 50)).forEach(i -> indexDoc(indexName, "id " + i, "f", i));
        flushAndRefresh(indexName);
        return indexName;
    }

    public static long assertBytesReadHeader(SearchRequest searchRequest) throws InterruptedException {
        return assertBytesReadHeader(searchRequest, null);
    }

    public static long assertBytesReadHeader(SearchRequest searchRequest, Consumer<SearchResponse> searchResponseConsumer)
        throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        client.search(searchRequest, new LatchedActionListener<>(ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                bytesRead.set(extractBytesReadHeader(client));
                if (searchResponseConsumer != null) {
                    searchResponseConsumer.accept(searchResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail("no error expected");
            }
        }), latch));
        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }

    public static long extractBytesReadHeader(Client client) {
        Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
        assertThat(responseHeaders, hasKey(StoreMetrics.BYTES_READ_RESPONSE_HEADER));
        List<String> values = responseHeaders.get(StoreMetrics.BYTES_READ_RESPONSE_HEADER);
        assertThat("expected a single accumulated header value", values.size(), equalTo(1));
        long total = Long.parseLong(values.get(0));
        assertThat(total, greaterThan(0L));
        return total;
    }
}
