/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.FieldBasedRerankerIT;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.search.SearchService.BATCHED_QUERY_PHASE;
import static org.elasticsearch.search.SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class SearchMetricsBytesReadIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FieldBasedRerankerIT.FieldBasedRerankerPlugin.class);
    }

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    // an index with twice the number of documents should always return a bigger number of bytes read
    public void testSingleShardWithDifferentSizes() throws InterruptedException {
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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));
        SearchRequest smallerIndexRequest = new SearchRequest(smallerIndex).source(searchSourceBuilder);
        long smallerIndexBytesRead = assertBytesReadHeader(smallerIndexRequest);

        SearchRequest biggerIndexRequest = new SearchRequest(biggerIndex).source(searchSourceBuilder);
        long biggerIndexBytesRead = assertBytesReadHeader(biggerIndexRequest);

        assertThat(smallerIndexBytesRead, greaterThan(0L));
        assertThat(biggerIndexBytesRead, greaterThan(0L));
        assertThat(biggerIndexBytesRead, greaterThan(smallerIndexBytesRead));
    }

    // TODO still fails at a 1% rate
    public void testMultiShardWithPreference() throws InterruptedException {
        // running a search with a preference should always be less than running the total search
        int numDocs = atLeast(200);
        String indexName = randomIndexName();
        createIndex(indexName, 2, 0);

        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            builders[i] = prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + "");
        }

        indexRandom(true, true, builders);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        SearchRequest fullSearchRequest = new SearchRequest(indexName).source(searchSourceBuilder);

        // initial warmup
        assertBytesReadHeader(fullSearchRequest);

        // full index, both shards
        long fullBytesRead = assertBytesReadHeader(fullSearchRequest);

        // preference, one shard
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder).preference("_shards:0");
        long preferenceBytesRead = assertBytesReadHeader(searchRequest);

        assertThat(preferenceBytesRead, greaterThan(10L));
        assertThat(fullBytesRead, greaterThan(preferenceBytesRead));
    }

    public void testBatchedExecution() throws InterruptedException {
        assumeTrue(
            "test skipped because batched query execution disabled by feature flag",
            SearchService.BATCHED_QUERY_PHASE_FEATURE_FLAG.isEnabled()
        );

        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            builders[i] = prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + "");
        }
        indexRandom(true, false, builders);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);

        // initial warmup
        assertBytesReadHeader(searchRequest);

        // run with batched execution enabled
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().put(BATCHED_QUERY_PHASE.getKey(), true))
        );

        long bytesReadBatchedExecutionEnabled = assertBytesReadHeader(searchRequest);

        // run with batched execution disabled
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().put(BATCHED_QUERY_PHASE.getKey(), false))
        );

        long bytesReadBatchedExecutionDisabled = assertBytesReadHeader(searchRequest);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().putNull(BATCHED_QUERY_PHASE.getKey()))
        );

        assertThat(bytesReadBatchedExecutionDisabled, equalTo(bytesReadBatchedExecutionEnabled));
    }

    public void testQueryPhaseParallelCollection() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        ;
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);

        // warm up
        assertBytesReadHeader(searchRequest);

        // run with parallel collection enabled
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().put(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), true))
        );

        long bytesReadBatchedExecutionEnabled = assertBytesReadHeader(searchRequest);

        // run with parallel collection disabled
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().put(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), false))
        );

        long bytesReadBatchedExecutionDisabled = assertBytesReadHeader(searchRequest);

        // reset again
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setTransientSettings(Settings.builder().putNull(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey()))
        );

        assertThat(bytesReadBatchedExecutionDisabled, equalTo(bytesReadBatchedExecutionEnabled));
    }

    public void testAggregations() throws InterruptedException {
        String indexName = randomIndexName();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings(1, 0).build())
                .setMapping("my-keyword", "type=keyword")
                .get()
        );
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("my-keyword", "keyword_" + (i % 10)));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("key").field("my-keyword"));

        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);
        long bytesRead = assertBytesReadHeader(searchRequest);
        assertThat(bytesRead, greaterThan(1000L));
    }

    public void testScrollSearch() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        ;
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);

        // warm up
        assertBytesReadHeader(searchRequest);

        long bytesRead = assertBytesReadHeader(searchRequest);

        // now as scroll search
        AtomicReference<String> scrollidReference = new AtomicReference<>();
        try {
            searchRequest = new SearchRequest(indexName).source(searchSourceBuilder).scroll(TimeValue.ONE_MINUTE);
            long bytesReadScrollSearch = assertBytesReadHeader(
                searchRequest,
                searchResponse -> scrollidReference.set(searchResponse.getScrollId())
            );
            assertThat(bytesReadScrollSearch, equalTo(bytesRead));

        } finally {
            client().prepareClearScroll().addScrollId(scrollidReference.get()).get();
        }
    }

    // TODO still fails at a 1% rate
    public void testPit() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);

        // warm up
        assertBytesReadHeader(searchRequest);

        long bytesRead = assertBytesReadHeader(searchRequest);

        // now as pit search
        var request = new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(between(1, 10)));
        BytesReference pit = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet().getPointInTimeId();

        searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .pointInTimeBuilder(new PointInTimeBuilder(pit))
            .sort("id", SortOrder.ASC);
        searchRequest = new SearchRequest().source(searchSourceBuilder);
        long pitBytesRead = assertBytesReadHeader(searchRequest);

        ClosePointInTimeResponse response = client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit))
            .actionGet();
        assertThat(response.getNumFreed(), equalTo(1));

        assertThat(bytesRead, equalTo(pitBytesRead));
    }

    public void testBytesReadNotLeakedAcrossRequests() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        // warmup
        assertBytesReadHeader(
            new SearchRequest(indexName).source(
                new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("id", SortOrder.ASC)
            )
        );

        SearchRequest largeRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("id", SortOrder.ASC).size(100)
        );
        long largeSearchBytesRead = assertBytesReadHeader(largeRequest);

        SearchRequest smallRequest = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).sort("id", SortOrder.ASC).size(1)
        );
        long smallSearchBytesRead = assertBytesReadHeader(smallRequest);

        assertThat(largeSearchBytesRead, greaterThan(smallSearchBytesRead));
    }

    public void testShardLevelExceptions() throws InterruptedException {
        String firstIndexName = randomIndexName();
        String secondIndexName = randomIndexName();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(firstIndexName)
                .setSettings(indexSettings(1, 0).build())
                .setMapping("number_or_keyword", "type=keyword")
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(secondIndexName)
                .setSettings(indexSettings(1, 0).build())
                .setMapping("number_or_keyword", "type=long")
                .get()
        );
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs * 2);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(firstIndexName).setSource("number_or_keyword", i));
            builders.add(prepareIndex(secondIndexName).setSource("number_or_keyword", i));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(firstIndexName, secondIndexName);

        // warm up
        assertBytesReadHeader(new SearchRequest(firstIndexName));
        assertBytesReadHeader(new SearchRequest(secondIndexName));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("number_or_keyword", "x"));

        long bytesReadFirstIndex = assertBytesReadHeader(new SearchRequest(firstIndexName).source(searchSourceBuilder));
        // no reads against the second index due to wrong mapping
        long bytesReadBothIndices = assertBytesReadHeader(new SearchRequest(firstIndexName, secondIndexName).source(searchSourceBuilder));

        assertThat(bytesReadFirstIndex, equalTo(bytesReadBothIndices));
    }

    public void testRankFeaturePhase() throws InterruptedException {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("rank_feature_field", (float) i / numDocs, "search_field", "value value" + i));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);

        // warmup
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("search_field", "value"));
        assertBytesReadHeader(new SearchRequest(indexName).source(searchSourceBuilder));

        long bytesReadWithoutRank = assertBytesReadHeader(new SearchRequest(indexName).source(searchSourceBuilder));

        SearchSourceBuilder rankedSearch = new SearchSourceBuilder().query(QueryBuilders.termQuery("search_field", "value"))
            .rankBuilder(new FieldBasedRerankerIT.FieldBasedRankBuilder(10, "rank_feature_field"));
        long bytesReadWithRank = assertBytesReadHeader(new SearchRequest(indexName).source(rankedSearch));

        assertThat(bytesReadWithRank, greaterThan(bytesReadWithoutRank));
    }

    private long assertBytesReadHeader(SearchRequest searchRequest) throws InterruptedException {
        return assertBytesReadHeader(searchRequest, (response) -> {});
    }

    private long assertBytesReadHeader(SearchRequest searchRequest, Consumer<SearchResponse> consumer) throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        client.search(searchRequest, ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
                    assertThat(responseHeaders, hasKey("X-Elasticsearch-Bytes-Read"));
                    assertThat(responseHeaders.get("X-Elasticsearch-Bytes-Read"), hasSize(1));
                    Long actual = Long.valueOf(responseHeaders.get("X-Elasticsearch-Bytes-Read").get(0));
                    assertThat(actual, greaterThan(10L));
                    bytesRead.set(actual);
                    consumer.accept(searchResponse);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                fail("no error expected");
            }
        }));
        latch.await(10, TimeUnit.SECONDS);
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }
}
