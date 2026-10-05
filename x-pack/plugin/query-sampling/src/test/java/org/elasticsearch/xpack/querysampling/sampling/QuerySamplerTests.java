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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class QuerySamplerTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1, 1);

    public void testNewQueryIsAcceptedWithAboutTwoThirdsOfTheScale() {
        QuerySampler sampler = new QuerySampler(0.5, 100, seededRandom());
        assertThat(sampler.acceptanceProbability(1), closeTo(0.5 * Math.log(2), 1e-12));
    }

    public void testProbabilityShrinksWithPopularityButAddsUpLogarithmically() {
        double scale = 0.5;
        QuerySampler sampler = new QuerySampler(scale, Long.MAX_VALUE, seededRandom());
        double total = 0;
        double previous = Double.MAX_VALUE;
        for (long arrival = 1; arrival <= 1000; arrival++) {
            double probability = sampler.acceptanceProbability(arrival);
            assertThat(probability, lessThan(previous));
            previous = probability;
            total += probability;
        }
        // the expected number of accepted arrivals after w of them is scale * ln(1 + w), not proportional to w
        assertThat(total, closeTo(scale * Math.log(1001), 1e-9));
    }

    public void testWeightedArrivalsAddUpToTheSameAsUnitOnes() {
        double scale = 0.2; // small enough for no probability to be capped at one
        QuerySampler sampler = new QuerySampler(scale, Long.MAX_VALUE, seededRandom());
        double weight = 10; // each captured arrival stands for ten
        double total = 0;
        double estimatedArrivals = 0;
        for (int arrival = 0; arrival < 100; arrival++) {
            estimatedArrivals += weight;
            total += sampler.acceptanceProbability(estimatedArrivals, weight);
        }
        assertThat(total, closeTo(scale * Math.log(1 + estimatedArrivals), 1e-9));
    }

    public void testProbabilityIsCappedAtOne() {
        assertThat(new QuerySampler(10, 100, seededRandom()).acceptanceProbability(1), equalTo(1.0));
    }

    public void testHeadQueriesAreAlwaysPicked() {
        QuerySampler sampler = new QuerySampler(0.01, 5, alwaysDrawing(0.999999));
        TrackedQuery query = tracked(5);
        assertTrue(sampler.offer(query));
        assertThat(query.inclusionProbability(), equalTo(1.0));
    }

    public void testAQueryIsPickedAtMostOnceButEveryArrivalCountsTowardsItsInclusionProbability() {
        QuerySampler sampler = new QuerySampler(0.5, 1000, alwaysDrawing(0.0));
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        TrackedQuery query = tracker.record(FINGERPRINT);
        assertTrue(sampler.offer(query));

        double survival = 1 - sampler.acceptanceProbability(1);
        assertThat(query.inclusionProbability(), closeTo(1 - survival, 1e-12));
        for (int arrival = 2; arrival <= 4; arrival++) {
            assertFalse("already picked", sampler.offer(tracker.record(FINGERPRINT)));
            survival *= 1 - sampler.acceptanceProbability(arrival);
        }
        assertThat(query.inclusionProbability(), closeTo(1 - survival, 1e-12));
    }

    /**
     * The inclusion probability is only useful as a weight if it is the real chance of the query ending up
     * in the sample, so compare it with how often that actually happens.
     */
    public void testInclusionProbabilityMatchesHowOftenQueriesAreSampled() {
        QuerySampler sampler = new QuerySampler(0.3, 1000, seededRandom());
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
            inclusionProbability = query.inclusionProbability();
        }
        double fiveSigma = 5 * Math.sqrt(inclusionProbability * (1 - inclusionProbability) / trials);
        assertThat((double) sampled / trials, closeTo(inclusionProbability, fiveSigma));
    }

    private static TrackedQuery tracked(int multiplicity) {
        MultiplicityTracker tracker = new MultiplicityTracker(1);
        TrackedQuery query = null;
        for (int i = 0; i < multiplicity; i++) {
            query = tracker.record(FINGERPRINT);
        }
        return query;
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
