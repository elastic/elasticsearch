/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.storage.Tier1Buffer;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingServiceTests extends ESTestCase {

    private final MultiplicityTracker tracker = new MultiplicityTracker(2);
    private final Tier1Buffer buffer = new Tier1Buffer(1);

    public void testNothingHappenedYet() {
        QuerySamplingService service = service(filter(1.0), handoff(Runnable::run, new Random(0L)));
        assertThat(service.stats(), equalTo(new QuerySamplingStats(0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    public void testGateCountersOnlyCountEligibleKnnSearches() {
        QueryCaptureFilter filter = filter(1.0);
        QuerySamplingService service = service(filter, handoff(Runnable::run, new Random(0L)));

        search(filter, knnSearch());
        search(filter, knnSearch());
        search(filter, new SearchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));

        QuerySamplingStats stats = service.stats();
        assertThat(stats.knnSearches(), equalTo(2L));
        assertThat(stats.captured(), equalTo(2L));
    }

    public void testStagesAreReflectedInTheStats() {
        CaptureHandoff handoff = handoff(Runnable::run, picking());
        QuerySamplingService service = service(filter(1.0), handoff);

        // two distinct queries fill the counter, the third is not tracked and a repeat is still counted
        handoff.accept(search(1f));
        handoff.accept(search(2f));
        handoff.accept(search(3f));
        handoff.accept(search(1f));

        QuerySamplingStats stats = service.stats();
        assertThat(stats.distinctQueries(), equalTo(2L));
        assertThat(stats.untrackedArrivals(), equalTo(1L));
        // both tracked queries were picked, but Tier 1 only has room for one
        assertThat(stats.picked(), equalTo(2L));
        assertThat(stats.buffered(), equalTo(1L));
        assertThat(stats.rejected(), equalTo(1L));
        assertThat(stats.dropped(), equalTo(0L));
    }

    public void testDroppedCapturesAreReported() {
        Executor full = command -> { throw new RejectedExecutionException("queue is full"); };
        CaptureHandoff handoff = handoff(full, picking());
        QuerySamplingService service = service(filter(1.0), handoff);

        handoff.accept(search(1f));
        handoff.accept(search(2f));

        QuerySamplingStats stats = service.stats();
        assertThat(stats.dropped(), equalTo(2L));
        assertThat(stats.distinctQueries(), equalTo(0L));
    }

    private CaptureHandoff handoff(Executor executor, Random random) {
        return new CaptureHandoff(executor, new SamplingPipeline(tracker, new QuerySampler(1.0, 100, random), List.of(buffer)));
    }

    private QuerySamplingService service(QueryCaptureFilter filter, CaptureHandoff handoff) {
        return new QuerySamplingService(filter, handoff, tracker, buffer);
    }

    private static Random picking() {
        return new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0; // every draw picks
            }
        };
    }

    private static QueryCaptureFilter filter(double captureRate) {
        Settings settings = Settings.builder()
            .put(QuerySamplingSettings.ENABLED.getKey(), true)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), captureRate)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE)
        );
        return new QueryCaptureFilter(clusterSettings, captured -> {});
    }

    /**
     * Sends a search through the gate. The rest of the chain never answers, which is enough as the gate counts
     * before the response is known.
     */
    private static void search(QueryCaptureFilter filter, SearchRequest request) {
        Task task = new Task(1, "transport", TransportSearchAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
        filter.apply(task, TransportSearchAction.NAME, request, ActionListener.<SearchResponse>noop(), (t, action, r, listener) -> {});
    }

    private static SearchRequest knnSearch() {
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", new float[] { randomFloat(), randomFloat() }, 10, 100, null, null, null);
        return new SearchRequest("idx").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
    }

    private static CapturedSearch search(float value) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { value }, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }
}
