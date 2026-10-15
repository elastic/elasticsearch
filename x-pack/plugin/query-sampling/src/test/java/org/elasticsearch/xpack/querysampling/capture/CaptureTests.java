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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class CaptureTests extends ESTestCase {

    private final List<CapturedSearch> captured = new ArrayList<>();

    public void testCapturesKnnSearch() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        float[] vector = randomVector(8);
        SearchRequest request = knnSearch(vector);

        assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));

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

    public void testFailedSearchIsNotCaptured() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Exception expected = new IllegalStateException("search failed");

        queryCaptureFilterApply(
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

        queryCaptureFilterApply(
            filter,
            knnSearch(randomVector(8)),
            TaskId.EMPTY_TASK_ID,
            respondWith(response(1, hit("idx", "1", 1f))),
            ActionListener.wrap(r -> delivered.set(true), e -> fail("unexpected failure"))
        );

        assertTrue(delivered.get());
    }

    public void testIgnoresSearchesThatAreNotOverAllTheDocumentsOfTheirIndicesOrDoNotReturnTheNearestK() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        List<SearchRequest> notComparable = new ArrayList<>();
        notComparable.add(knnSearch(randomVector(8)).routing("a"));
        notComparable.add(knnSearch(randomVector(8)).preference("_shards:0"));
        notComparable.add(knnSearch(randomVector(8)).scroll(TimeValue.timeValueMinutes(1)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().from(5)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().minScore(0.5f)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().postFilter(QueryBuilders.termQuery("category", 1))));
        for (SearchRequest request : notComparable) {
            assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));
        }

        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
        // a shard preference that does not leave shards out is still a search of everything
        assertTrue(queryCaptureFilterApply(filter, knnSearch(randomVector(8)).preference("_local"), TaskId.EMPTY_TASK_ID));
        assertThat(filter.knnSearches(), equalTo(1L));
    }

    public void testTheRecallIsToldOverTheHitsThatTheSearchReturns() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", randomVector(8), 100, 1000, null, null, null);
        // k of 100 with the default size of 10, k of 100 with a size of 5, and a size larger than k
        for (Integer size : new Integer[] { null, 5, 200 }) {
            SearchSourceBuilder source = new SearchSourceBuilder().knnSearch(List.of(knn));
            if (size != null) {
                source.size(size);
            }
            assertTrue(queryCaptureFilterApply(filter, new SearchRequest("idx").source(source), TaskId.EMPTY_TASK_ID));
        }

        assertThat(captured.stream().map(search -> search.query().k()).toList(), equalTo(List.of(10, 5, 100)));
    }

    public void testCapturedSearchRemembersTheRateItWasDrawnAt() {
        ClusterSettings clusterSettings = clusterSettings(true, 1.0);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, captured::add);
        queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);

        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), true)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 0.5)
                .build()
        );
        int searches = 100;
        for (int i = 0; i < searches; i++) {
            queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
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
        return new ClusterSettings(
            settings,
            Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE, QuerySamplingSettings.MIN_CAPTURES_PER_HOUR)
        );
    }

    private static SearchSourceBuilder knnSource() {
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", randomVector(8), 10, 100, null, null, null);
        return new SearchSourceBuilder().knnSearch(List.of(knn));
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
            searchHits.decRef();
        }
    }

    private static Consumer<ActionListener<SearchResponse>> respondWith(SearchResponse response) {
        return listener -> {
            try {
                listener.onResponse(response);
            } finally {
                response.decRef();
            }
        };
    }

    private static boolean queryCaptureFilterApply(QueryCaptureFilter filter, SearchRequest request, TaskId parent) {
        return queryCaptureFilterApply(filter, request, parent, respondWith(response(1)), ActionListener.noop());
    }

    private static boolean queryCaptureFilterApply(
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

    public void testQuietTrafficGetsARateThatMeetsTheFloor() {
        CaptureRate rate = new CaptureRate(0.01, 360);

        rate.observe(1, 1);

        assertThat(rate.effective(), closeTo(0.1, 1e-12));
    }

    public void testDropsWithoutBlockingWhenTheQueueIsFull() throws Exception {
        EsThreadPoolExecutor executor = executor(1, 2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        try {
            CaptureHandoff handoff = new CaptureHandoff(executor, captureHandoffCaptured -> {
                started.countDown();
                safeAwait(release);
                processed.incrementAndGet();
            });

            handoff.accept(captureHandoffCaptured());
            safeAwait(started);
            handoff.accept(captureHandoffCaptured());
            handoff.accept(captureHandoffCaptured());
            int overflow = between(1, 20);
            for (int i = 0; i < overflow; i++) {
                handoff.accept(captureHandoffCaptured());
            }

            assertThat(handoff.handedOff(), equalTo(3L));
            assertThat(handoff.dropped(), equalTo((long) overflow));

            release.countDown();
            assertBusy(() -> assertThat(processed.get(), equalTo(3)));
        } finally {
            release.countDown();
            terminate(executor);
        }
    }

    private EsThreadPoolExecutor executor(int threads, int queueSize) {
        return EsExecutors.newFixed(
            getTestName(),
            threads,
            queueSize,
            EsExecutors.daemonThreadFactory("test", getTestName()),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
    }

    private static void terminate(EsThreadPoolExecutor executor) {
        executor.shutdownNow();
    }

    private static CapturedSearch captureHandoffCaptured() {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }
}
