/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class QuerySamplerTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1, 1);

    public void testQueriesGetNoChanceOnceThePicksAreUsedUpAndThatIsRecorded() {
        AtomicLong clock = new AtomicLong();
        PickBudget limit = new PickBudget(clock::get);
        limit.perHour(60);
        QuerySampler sampler = new QuerySampler(1.0, 1000, alwaysDrawing(0.0), limit, new SpatialStrata(0));
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        TrackedQuery first = tracker.record(new QueryFingerprint(1, 1));
        TrackedQuery second = tracker.record(new QueryFingerprint(2, 2));
        TrackedQuery third = tracker.record(new QueryFingerprint(3, 3));

        assertTrue("the pick there is", sampler.offer(first));
        assertFalse(sampler.offer(second));
        assertFalse(sampler.offer(third));
        assertThat("no chance, which is what the probability of the query says", second.weights().inclusionProbability(), equalTo(0.0));
        assertThat("both were turned away by the limit", sampler.starved(), equalTo(2L));
        assertThat(first.weights().inclusionProbability(), greaterThan(0.0));

        clock.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertTrue("a pick is back", sampler.offer(tracker.record(new QueryFingerprint(4, 4))));
    }

    public void testInclusionProbabilityMatchesHowOftenQueriesAreSampled() {
        QuerySampler sampler = new QuerySampler(0.3, 1000, seededRandom(), new PickBudget(System::nanoTime), new SpatialStrata(0));
        int arrivals = 6;
        int trials = 20_000;
        int sampled = 0;
        double inclusionProbability = 0;
        for (int trial = 0; trial < trials; trial++) {
            MultiplicityTracker tracker = new MultiplicityTracker(1);
            TrackedQuery query = null;
            for (int arrival = 0; arrival < arrivals; arrival++) {
                query = tracker.record(FINGERPRINT);
                sampler.offer(query);
            }
            sampled += query.isSampled() ? 1 : 0;
            inclusionProbability = query.weights().inclusionProbability();
        }
        double fiveSigma = 5 * Math.sqrt(inclusionProbability * (1 - inclusionProbability) / trials);
        assertThat((double) sampled / trials, closeTo(inclusionProbability, fiveSigma));
    }

    private Random seededRandom() {
        return new Random(randomLong());
    }

    private static Random alwaysDrawing(double value) {
        return new Random(0L) {
            @Override
            public double nextDouble() {
                return value;
            }
        };
    }
}
