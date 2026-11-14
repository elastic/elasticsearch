/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.sampling.EventSlice;
import org.elasticsearch.xpack.querysampling.sampling.PickBudget;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SelectivityStrata;
import org.elasticsearch.xpack.querysampling.sampling.SpatialStrata;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SamplingPipelineTests extends ESTestCase {

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    private final List<SampledQuery> sampled = new ArrayList<>();

    public void testRepeatedQueryIsPickedOnceAndKeepsBeingCounted() {
        SamplingPipeline pipeline = new SamplingPipeline(
            tracker,
            sampler(1.0, 0.0),
            List.of(sampled::add),
            new CostBudget(0.0, 0.0),
            new EventSlice(),
            noSelectivity()
        );
        float[] vector = { 1f, 2f, 3f };

        for (int i = 0; i < 5; i++) {
            pipeline.accept(search(vector));
        }

        assertThat(sampled.size(), equalTo(1));
        assertThat(pipeline.picked(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(1));
    }

    public void testSomeCapturedSearchesAreAlsoKeptAsEventsWhateverHappensToTheQuery() {
        EventSlice events = new EventSlice(() -> picking(0.0));
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), 0.25).build(),
            Set.of(QuerySamplingSettings.EVENT_SLICE_RATE)
        );
        events.watch(clusterSettings);
        MultiplicityTracker tiny = new MultiplicityTracker(1);
        SamplingPipeline pipeline = new SamplingPipeline(
            tiny,
            sampler(0.1, 0.999999),
            List.of(sampled::add),
            new CostBudget(0.0, 0.0),
            events,
            noSelectivity()
        );
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);

        pipeline.accept(new CapturedSearch(query, List.of(), 1, 0.5));
        pipeline.accept(new CapturedSearch(query, List.of(), 1, 0.5));
        pipeline.accept(search(new float[] { 2f }));

        assertThat(pipeline.picked(), equalTo(0L));
        assertThat(sampled.size(), equalTo(3));
        assertTrue(sampled.stream().allMatch(SampledQuery::isEvent));
        assertThat("two events of the same query", sampled.get(0).eventId().equals(sampled.get(1).eventId()), equalTo(false));
        assertThat(
            "captured with 0.5 and kept with 0.25",
            sampled.get(0).tracked().weights().inclusionProbability(),
            closeTo(0.125, 1e-12)
        );
        assertThat("each is one search, standing for two", sampled.get(0).tracked().weights().weightedMultiplicity(), closeTo(2.0, 1e-12));
    }

    private static QuerySampler sampler(double scale, double draw) {
        return new QuerySampler(scale, 100, picking(draw), new PickBudget(System::nanoTime), new SpatialStrata(0));
    }

    /** Selectivity counts are off in these tests; if one were asked for it would fail rather than search. */
    private static SelectivityStrata noSelectivity() {
        return new SelectivityStrata(
            (request, listener) -> listener.onFailure(new UnsupportedOperationException()),
            () -> 0L,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    /** A random whose draws are all the given value, so sampling decisions are deterministic. */
    private static Random picking(double draw) {
        return new Random(0L) {
            @Override
            public double nextDouble() {
                return draw;
            }
        };
    }

    private static CapturedSearch search(float[] vector) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", vector, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }
}
