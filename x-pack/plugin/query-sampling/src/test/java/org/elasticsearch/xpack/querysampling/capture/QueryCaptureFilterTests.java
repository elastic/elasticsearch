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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class QueryCaptureFilterTests extends ESTestCase {

    private final List<CapturedSearch> captured = new ArrayList<>();

    public void testCapturesKnnSearch() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        float[] vector = randomVector(8);
        SearchRequest request = knnSearch(vector);

        assertTrue(applyFilter(filter, request, TaskId.EMPTY_TASK_ID));

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

    public void testIgnoresSearchesThatAreNotOverAllTheDocumentsOfTheirIndicesOrDoNotReturnTheNearestK() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);
        List<SearchRequest> notComparable = new ArrayList<>();
        notComparable.add(knnSearch(randomVector(8)).routing("a"));
        notComparable.add(knnSearch(randomVector(8)).preference("_shards:0"));
        notComparable.add(knnSearch(randomVector(8)).preference("_local"));
        notComparable.add(knnSearch(randomVector(8)).preference("_only_local"));
        notComparable.add(knnSearch(randomVector(8)).preference("session-7"));
        notComparable.add(knnSearch(randomVector(8)).scroll(TimeValue.timeValueMinutes(1)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().from(5)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().minScore(0.5f)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().sort("category"))); // its hits have no score
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().postFilter(QueryBuilders.termQuery("category", 1))));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().searchAfter(new Object[] { 1 })));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().terminateAfter(5)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().indexBoost("idx", 2f)));
        notComparable.add(knnSearch(randomVector(8)).source(knnSource().size(0)));
        for (SearchRequest request : notComparable) {
            assertTrue(applyFilter(filter, request, TaskId.EMPTY_TASK_ID));
        }

        assertTrue(captured.isEmpty());
        assertThat(filter.knnSearches(), equalTo(0L));
    }

    public void testASearchSortedByItsScoreIsTheDefaultOrderAndIsCaptured() {
        QueryCaptureFilter filter = filter(true, 1.0, captured::add);

        assertTrue(
            applyFilter(filter, knnSearch(randomVector(8)).source(knnSource().sort(SortBuilders.scoreSort())), TaskId.EMPTY_TASK_ID)
        );

        assertThat(captured.size(), equalTo(1));
    }

    public void testTheCaptureRateFollowsTheTrafficWhenSearchesComeInWithoutAScheduledTask() {
        AtomicLong millis = new AtomicLong();
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings(true, 0.0, 3600), captured::add, millis::get);
        assertThat("no traffic known yet, and none configured", filter.effectiveCaptureRate(), equalTo(0.0));

        for (int i = 0; i < 20; i++) {
            assertTrue(applyFilter(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID));
        }
        assertThat("an interval has not passed", filter.effectiveCaptureRate(), equalTo(0.0));

        millis.addAndGet(TimeUnit.SECONDS.toMillis(10));
        assertTrue(applyFilter(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID));
        assertThat("21 searches in 10 s, and 3600 captures an hour are wanted", filter.effectiveCaptureRate(), closeTo(1 / 2.1, 1e-9));

        millis.addAndGet(TimeUnit.SECONDS.toMillis(60));
        assertTrue(applyFilter(filter, knnSearch(randomVector(8)), TaskId.EMPTY_TASK_ID));
        assertThat(
            "after a quiet spell what came before is forgotten, and whatever arrives is wanted",
            filter.effectiveCaptureRate(),
            equalTo(1.0)
        );
    }

    private static QueryCaptureFilter filter(boolean enabled, double rate, Consumer<CapturedSearch> consumer) {
        return new QueryCaptureFilter(clusterSettings(enabled, rate), consumer, () -> 0L);
    }

    private static ClusterSettings clusterSettings(boolean enabled, double rate) {
        return clusterSettings(enabled, rate, 0);
    }

    private static ClusterSettings clusterSettings(boolean enabled, double rate, long minCapturesPerHour) {
        Settings settings = Settings.builder()
            .put(QuerySamplingSettings.ENABLED.getKey(), enabled)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), rate)
            .put(QuerySamplingSettings.MIN_CAPTURES_PER_HOUR.getKey(), minCapturesPerHour)
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

    private static boolean applyFilter(QueryCaptureFilter filter, SearchRequest request, TaskId parent) {
        return applyFilter(filter, request, parent, respondWith(response(1)), ActionListener.noop());
    }

    private static boolean applyFilter(
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
