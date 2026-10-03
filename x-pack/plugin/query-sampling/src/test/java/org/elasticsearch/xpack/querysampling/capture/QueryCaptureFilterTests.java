/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class QueryCaptureFilterTests extends ESTestCase {

    private final List<CapturedSearch> captured = new ArrayList<>();

    public void testCapturesKnnSearch() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        float[] vector = randomVector(8);
        SearchRequest request = knnSearch(vector);

        assertTrue(apply(filter, request, TaskId.EMPTY_TASK_ID));

        assertThat(captured.size(), equalTo(1));
        CapturedQuery query = captured.get(0).query();
        assertArrayEquals(new String[] { "idx" }, query.indices());
        assertThat(query.field(), equalTo("vec"));
        assertArrayEquals(vector, query.queryVector(), 0f);
        assertThat("the vector must be copied out of the request", query.queryVector(), not(sameInstance(vector)));
        assertThat(query.k(), equalTo(10));
        assertThat(query.numCandidates(), equalTo(100));
        assertThat(query.oversample(), equalTo(2f));
        assertThat(query.filters(), equalTo(List.of(QueryBuilders.termQuery("category", 3))));
        assertThat(query.opaqueId(), equalTo("q7"));
    }

    public void testCapturesWhatTheSearchReturned() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchResponse response = response(7, hit("idx", "42", 0.9f), hit("idx", "7", 0.8f));

        apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID, respondWith(response), ActionListener.noop());

        assertThat(captured.size(), equalTo(1));
        assertThat(captured.get(0).tookMillis(), equalTo(7L));
        assertThat(
            captured.get(0).hits(),
            equalTo(List.of(new CapturedSearch.Hit("idx", "42", 0.9f), new CapturedSearch.Hit("idx", "7", 0.8f)))
        );
    }

    public void testFailedSearchIsNotCaptured() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Exception expected = new IllegalStateException("search failed");

        apply(
            filter,
            knnSearch(randomVector(8)),
            TaskId.EMPTY_TASK_ID,
            listener -> listener.onFailure(expected),
            ActionListener.wrap(r -> fail("unexpected response"), failure::set)
        );

        assertThat(failure.get(), sameInstance(expected));
        assertTrue(captured.isEmpty());
    }

    public void testFailingConsumerStillDeliversTheResponse() {
        QueryCaptureFilter filter = filter(true, 1.0, search -> { throw new IllegalStateException("boom"); });
        AtomicBoolean delivered = new AtomicBoolean();

        apply(
            filter,
            knnSearch(randomVector(8)),
            TaskId.EMPTY_TASK_ID,
            respondWith(response(1, hit("idx", "1", 1f))),
            ActionListener.wrap(r -> delivered.set(true), e -> fail("unexpected failure"))
        );

        assertTrue(delivered.get());
    }

    public void testNothingCapturedWhenDisabled() {
        QueryCaptureFilter filter = filter(false, 1.0, captured::add);
        assertTrue(apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
    }

    public void testIgnoresSearchesWithoutKnn() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchRequest request = new SearchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        assertTrue(apply(filter, request, TaskId.EMPTY_TASK_ID));
        assertTrue(apply(filter, new SearchRequest("idx"), TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
    }

    public void testIgnoresHybridSearches() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchRequest request = knnSearch(randomVector(8));
        request.source().query(QueryBuilders.matchQuery("title", "phone"));
        assertTrue(apply(filter, request, TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
    }

    public void testIgnoresChildSearches() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        assertTrue(apply(filter, knnSearch(randomVector(8)), new TaskId("remote-node", randomNonNegativeLong())));
        assertTrue(captured.isEmpty());
    }

    public void testCaptureRate() {
        double rate = randomDoubleBetween(0.1, 0.9, true);
        int searches = 4000;
        QueryCaptureFilter filter = filter(true, rate, captured::add);
        for (int i = 0; i < searches; i++) {
            apply(filter, knnSearch(randomVector(4)), TaskId.EMPTY_TASK_ID);
        }
        double expected = rate * searches;
        double fiveSigma = 5 * Math.sqrt(searches * rate * (1 - rate));
        assertThat((double) captured.size(), both(greaterThan(expected - fiveSigma)).and(lessThan(expected + fiveSigma)));
    }

    public void testSettingsAreDynamic() {
        ClusterSettings clusterSettings = clusterSettings(false, 0.0);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, captured::add);
        apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        assertTrue(captured.isEmpty());

        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), true)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 1.0)
                .build()
        );
        apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        assertThat(captured.size(), equalTo(1));
    }

    public void testCapturedSearchRemembersTheRateItWasDrawnAt() {
        ClusterSettings clusterSettings = clusterSettings(true, 1.0);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, captured::add);
        apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);

        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), true)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 0.5)
                .build()
        );
        // a search that passes the lowered gate says so, so the rate cannot be taken from the current setting
        int searches = 100;
        for (int i = 0; i < searches; i++) {
            apply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        }

        assertThat(captured.get(0).captureRate(), equalTo(1.0));
        assertThat(captured.size(), greaterThan(1));
        for (CapturedSearch search : captured.subList(1, captured.size())) {
            assertThat(search.captureRate(), equalTo(0.5));
        }
    }

    private static QueryCaptureFilter filter(boolean enabled, double rate, Consumer<CapturedSearch> consumer) {
        return new QueryCaptureFilter(clusterSettings(enabled, rate), consumer);
    }

    private static ClusterSettings clusterSettings(boolean enabled, double rate) {
        Settings settings = Settings.builder()
            .put(QuerySamplingSettings.ENABLED.getKey(), enabled)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), rate)
            .build();
        return new ClusterSettings(settings, Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE));
    }

    private static SearchRequest knnSearch(float[] vector) {
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", vector, 10, 100, null, new RescoreVectorBuilder(2f), null).addFilterQuery(
            QueryBuilders.termQuery("category", 3)
        );
        return new SearchRequest("idx").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
    }

    private static SearchHit hit(String index, String id, float score) {
        SearchHit hit = SearchHit.unpooled(randomNonNegativeInt(), id);
        hit.score(score);
        hit.shard(new SearchShardTarget("node", new ShardId(index, "_na_", 0), null));
        return hit;
    }

    private static SearchResponse response(long tookMillis, SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
        try {
            return SearchResponseUtils.response(searchHits).tookInMillis(tookMillis).build();
        } finally {
            searchHits.decRef(); // the response holds its own reference
        }
    }

    /**
     * What the rest of the chain does with the search: answers with the response, which is released once
     * the listener has seen it.
     */
    private static Consumer<ActionListener<SearchResponse>> respondWith(SearchResponse response) {
        return listener -> {
            try {
                listener.onResponse(response);
            } finally {
                response.decRef();
            }
        };
    }

    /**
     * Runs the filter and returns whether the search was passed on down the chain.
     */
    private static boolean apply(QueryCaptureFilter filter, SearchRequest request, TaskId parent) {
        return apply(filter, request, parent, respondWith(response(1)), ActionListener.noop());
    }

    private static boolean apply(
        QueryCaptureFilter filter,
        SearchRequest request,
        TaskId parent,
        Consumer<ActionListener<SearchResponse>> restOfChain,
        ActionListener<SearchResponse> downstream
    ) {
        Task task = new Task(1, "transport", TransportSearchAction.NAME, "", parent, Map.of(Task.X_OPAQUE_ID_HTTP_HEADER, "q7"));
        AtomicBoolean proceeded = new AtomicBoolean();
        ActionFilterChain<SearchRequest, SearchResponse> chain = (t, action, r, listener) -> {
            proceeded.set(true);
            restOfChain.accept(listener);
        };
        filter.apply(task, TransportSearchAction.NAME, request, downstream, chain);
        return proceeded.get();
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
