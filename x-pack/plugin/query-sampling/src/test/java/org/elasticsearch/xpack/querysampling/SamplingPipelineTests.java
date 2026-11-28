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
import org.elasticsearch.xpack.querysampling.sampling.PickBudget;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SampledQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class SamplingPipelineTests extends ESTestCase {

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    private final List<SampledQuery> sampled = new ArrayList<>();

    public void testRepeatedQueryIsPickedOnceAndKeepsBeingCounted() {
        SamplingPipeline pipeline = new SamplingPipeline(tracker, sampler(1.0, 0.0), List.of(sampled::add));
        float[] vector = { 1f, 2f, 3f };

        for (int i = 0; i < 5; i++) {
            pipeline.accept(search(vector));
        }

        assertThat(sampled.size(), equalTo(1));
        assertThat(pipeline.picked(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(1));
    }

    private static QuerySampler sampler(double scale, double draw) {
        Random random = picking(draw);
        return new QuerySampler(scale, 100, () -> random, new PickBudget(System::nanoTime));
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
