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
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class QuerySamplerTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1, 1);

    public void testQueriesGetALowerChanceOnceThePicksAreUsedUpAndThatIsRecorded() {
        AtomicLong clock = new AtomicLong();
        PickBudget limit = new PickBudget(clock::get);
        limit.perHour(60);
        QuerySampler sampler = new QuerySampler(1.0, 1000, supplying(alwaysDrawing(0.5)), limit, new SpatialStrata(0));
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        TrackedQuery first = tracker.record(new QueryFingerprint(1, 1));
        TrackedQuery second = tracker.record(new QueryFingerprint(2, 2));
        TrackedQuery third = tracker.record(new QueryFingerprint(3, 3));

        assertTrue("the pick there is", sampler.offer(first));
        assertFalse(sampler.offer(second));
        assertFalse(sampler.offer(third));
        assertThat(
            "a chance cut, not taken away, which is what the probability of the query says",
            second.weights().inclusionProbability(),
            closeTo(QuerySampler.LIMITED_FACTOR * Math.log(2), 1e-12)
        );
        assertThat("both were cut by the limit", sampler.starved(), equalTo(2L));
        assertThat(first.weights().inclusionProbability(), greaterThan(0.0));

        clock.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertTrue("a pick is back", sampler.offer(tracker.record(new QueryFingerprint(4, 4))));
    }

    public void testAQueryThatIsPickedKeepsRecordingItsChancesAgainstTheLimitAsItIs() {
        AtomicLong clock = new AtomicLong();
        PickBudget limit = new PickBudget(clock::get);
        limit.perHour(60); // a single pick
        QuerySampler sampler = new QuerySampler(1.0, 1000, supplying(alwaysDrawing(0.0)), limit, new SpatialStrata(0));
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        assertTrue("picked, which uses the only pick up", sampler.offer(tracker.record(FINGERPRINT)));
        TrackedQuery again = tracker.record(FINGERPRINT);
        assertFalse("it is picked already", sampler.offer(again));

        double first = Math.log(2); // γ·ln((1 + 1) / (1 + 1 − 1))
        double second = QuerySampler.LIMITED_FACTOR * Math.log(1.5); // γ·ln((1 + 2) / (1 + 2 − 1)), cut as the limit is used up
        assertThat(
            "the limit is shared, so a query is cut when it is used up, also by the pick of the query itself",
            again.weights().inclusionProbability(),
            closeTo(1 - (1 - first) * (1 - second), 1e-12)
        );
        assertThat(sampler.starved(), equalTo(1L));
    }

    public void testInclusionProbabilityMatchesHowOftenQueriesAreSampled() {
        QuerySampler sampler = new QuerySampler(
            0.3,
            1000,
            supplying(seededRandom()),
            new PickBudget(System::nanoTime),
            new SpatialStrata(0)
        );
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

    private static Supplier<Random> supplying(Random random) {
        return () -> random;
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
