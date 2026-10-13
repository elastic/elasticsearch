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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
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

    public void testCapturesWhatTheSearchReturned() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchResponse response = response(7, hit("idx", "42", 0.9f), hit("idx", "7", 0.8f));

        queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID, respondWith(response), ActionListener.noop());

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

    public void testNothingCapturedWhenDisabled() {
        QueryCaptureFilter filter = filter(false, 1.0, captured::add);
        assertTrue(queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
    }

    public void testIgnoresSearchesWithoutKnn() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchRequest request = new SearchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));
        assertTrue(queryCaptureFilterApply(filter, new SearchRequest("idx"), TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
    }

    public void testFloorRaisesTheRateOfQuietTraffic() {
        ClusterSettings clusterSettings = clusterSettings(true, 0.0);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, captured::add);
        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), true)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 0.0)
                .put(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR.getKey(), 720)
                .build()
        );
        assertThat("the traffic is not known yet", filter.effectiveCaptureRate(), equalTo(0.0));

        queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        filter.updateRate(1.0);
        assertThat(filter.effectiveCaptureRate(), closeTo(0.1, 1e-12));

        captured.clear();
        int searches = 4000;
        for (int i = 0; i < searches; i++) {
            queryCaptureFilterApply(filter, knnSearch(randomVector(4)), TaskId.EMPTY_TASK_ID);
        }
        double fiveSigma = 5 * Math.sqrt(searches * 0.1 * 0.9);
        assertThat((double) captured.size(), both(greaterThan(400 - fiveSigma)).and(lessThan(400 + fiveSigma)));
        assertTrue("what was captured says the rate it was captured with", captured.stream().allMatch(c -> c.captureRate() == 0.1));
    }

    public void testNothingIsKnownAboutTheTrafficWhileSamplingIsOff() {
        ClusterSettings clusterSettings = clusterSettings(true, 0.0);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, captured::add);
        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), true)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 0.0)
                .put(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR.getKey(), 720)
                .build()
        );
        queryCaptureFilterApply(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID);
        filter.updateRate(1.0);
        assertThat(filter.effectiveCaptureRate(), greaterThan(0.0));

        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ENABLED.getKey(), false)
                .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), 0.0)
                .put(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR.getKey(), 720)
                .build()
        );
        filter.updateRate(1.0);

        assertThat("it would be wrong when sampling is switched on again", filter.effectiveCaptureRate(), equalTo(0.0));
    }

    public void testIgnoresHybridSearches() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        SearchRequest request = knnSearch(randomVector(8));
        request.source().query(QueryBuilders.matchQuery("title", "phone"));
        assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
    }

    public void testIgnoresSearchesWithASimilarityThreshold() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", randomVector(8), 10, 100, null, null, randomFloat());
        SearchRequest request = new SearchRequest("idx").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
        assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));
        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
    }

    public void testIgnoresSearchesThatAreNotOverAllTheDocumentsOfTheirIndicesOrDoNotReturnTheNearestK() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        List<SearchRequest> notComparable = new ArrayList<>();
        notComparable.add(knnSearch(randomVector(8)).routing("a"));
        notComparable.add(knnSearch(randomVector(8)).preference("_shards:0"));
        notComparable.add(knnSearch(randomVector(8)).scroll(TimeValue.timeValueMinutes(1)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().from(5)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().size(5)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().minScore(0.5f)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().postFilter(QueryBuilders.termQuery("category", 1))));
        for (SearchRequest request : notComparable) {
            assertTrue(queryCaptureFilterApply(filter, request, TaskId.EMPTY_TASK_ID));
        }

        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
        // a shard preference that does not leave shards out, and a size of at least k, are still searches of everything
        assertTrue(queryCaptureFilterApply(filter, knnSearch(randomVector(8)).preference("_local"), TaskId.EMPTY_TASK_ID));
        assertTrue(queryCaptureFilterApply(filter, knnSearch(randomVector(8)).source(knnSource().size(20)), TaskId.EMPTY_TASK_ID));
        assertThat(filter.knnSearches(), equalTo(2L));
    }

    public void testIgnoresChildSearches() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        assertTrue(queryCaptureFilterApply(filter, knnSearch(randomVector(8)), new TaskId("remote-node", randomNonNegativeLong())));
        assertTrue(captured.isEmpty());
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

    public void testWithoutAFloorItIsTheConfiguredRate() {
        CaptureRate rate = new CaptureRate(0.2, 0);

        rate.observe(5, 1);
        assertThat(rate.effective(), equalTo(0.2));
        rate.observe(0, 1);
        assertThat("however quiet", rate.effective(), equalTo(0.2));
    }

    public void testQuietTrafficGetsARateThatMeetsTheFloor() {
        CaptureRate rate = new CaptureRate(0.01, 360);

        rate.observe(1, 1);

        assertThat(rate.effective(), closeTo(0.1, 1e-12));
    }

    public void testTheRateNeverGoesAboveOne() {
        CaptureRate rate = new CaptureRate(0.01, 1_000_000);

        rate.observe(1, 1);
        assertThat(rate.effective(), equalTo(1.0));
        rate.observe(0, 1);
        assertThat("and no traffic means anything that comes is captured", rate.effective(), equalTo(1.0));
    }

    public void testTheArrivalRateIsSmoothed() {
        CaptureRate rate = new CaptureRate(0.0, 3600);

        rate.observe(100, 1);
        assertThat("the first observation is all there is", rate.effective(), closeTo(3600 / (100.0 * 3600), 1e-12));
        rate.observe(0, 1);
        assertThat(rate.effective(), closeTo(3600 / (70.0 * 3600), 1e-12));
    }

    public void testSettingsTakeEffectAtOnce() {
        CaptureRate rate = new CaptureRate(0.01, 0);
        rate.observe(1, 1);
        assertThat(rate.effective(), equalTo(0.01));

        rate.minPerHour(360);
        assertThat(rate.effective(), closeTo(0.1, 1e-12));

        rate.configured(0.3);
        assertThat(rate.effective(), equalTo(0.3));

        rate.minPerHour(0);
        rate.configured(0.02);
        assertThat(rate.effective(), equalTo(0.02));
    }

    public void testForgettingTheTrafficGoesBackToTheConfiguredRate() {
        CaptureRate rate = new CaptureRate(0.01, 360);
        rate.observe(1, 1);
        assertThat(rate.effective(), closeTo(0.1, 1e-12));

        rate.forgetTraffic();

        assertThat(rate.effective(), equalTo(0.01));
    }

    public void testHandsOffToTheExecutor() throws Exception {
        EsThreadPoolExecutor executor = executor(1, 10);
        try {
            CountDownLatch processed = new CountDownLatch(3);
            CaptureHandoff handoff = new CaptureHandoff(executor, captureHandoffCaptured -> processed.countDown());

            for (int i = 0; i < 3; i++) {
                handoff.accept(captureHandoffCaptured());
            }

            assertTrue(processed.await(10, TimeUnit.SECONDS));
            assertThat(handoff.handedOff(), equalTo(3L));
            assertThat(handoff.dropped(), equalTo(0L));
        } finally {
            terminate(executor);
        }
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
