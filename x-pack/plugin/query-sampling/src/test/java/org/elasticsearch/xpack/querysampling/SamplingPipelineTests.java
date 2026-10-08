/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SamplingPipelineTests extends ESTestCase {

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);
    private final List<SampledQuery> sampled = new ArrayList<>();

    public void testRepeatedQueryIsPickedOnceAndKeepsBeingCounted() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0; // every draw picks
            }
        });
        float[] vector = { 1f, 2f, 3f };

        for (int i = 0; i < 5; i++) {
            pipeline.accept(search(vector));
        }

        assertThat(sampled.size(), equalTo(1));
        assertThat(pipeline.picked(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(1));
    }

    public void testDistinctQueriesArePickedSeparately() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });

        pipeline.accept(search(new float[] { 1f }));
        pipeline.accept(search(new float[] { 2f }));
        pipeline.accept(search(new float[] { 3f }));

        assertThat(sampled.size(), equalTo(3));
        assertThat(pipeline.picked(), equalTo(3L));
        assertThat(tracker.distinct(), equalTo(3));
    }

    public void testNothingIsPassedOnWhenNothingIsPicked() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.999999; // never below the acceptance probability
            }
        });

        for (int i = 0; i < 10; i++) {
            pipeline.accept(search(new float[] { i }));
        }

        assertThat(sampled.size(), equalTo(0));
        assertThat(pipeline.picked(), equalTo(0L));
        assertThat(tracker.distinct(), equalTo(10));
    }

    public void testEveryListenerIsToldAndAFailingOneDoesNotStopTheOthers() {
        List<SampledQuery> first = new ArrayList<>();
        List<SampledQuery> last = new ArrayList<>();
        SamplingPipeline pipeline = new SamplingPipeline(tracker, new QuerySampler(1.0, 100, new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        }), List.of(first::add, query -> { throw new IllegalStateException("listener failed"); }, last::add));

        pipeline.accept(search(new float[] { 1f }));

        assertThat(first.size(), equalTo(1));
        assertThat(last.size(), equalTo(1));
        assertThat(last.get(0), sameInstance(first.get(0)));
    }

    private SamplingPipeline pipeline(Random random) {
        return new SamplingPipeline(tracker, new QuerySampler(1.0, 100, random), List.of(sampled::add));
    }

    private static CapturedSearch search(float[] vector) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", vector, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }
}
