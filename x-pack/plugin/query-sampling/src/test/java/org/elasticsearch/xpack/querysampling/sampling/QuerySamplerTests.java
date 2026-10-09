/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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

    public void testFollowsTheSettingsWhenTheyChange() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(QuerySamplingSettings.ACCEPTANCE_SCALE.getKey(), 0.5)
                .put(QuerySamplingSettings.HEAD_THRESHOLD.getKey(), 5)
                .build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR
            )
        );
        QuerySampler sampler = new QuerySampler(1.0, 100, seededRandom());
        sampler.watch(clusterSettings);

        // what is set when the sampler starts to watch replaces what it was created with
        assertThat(sampler.acceptanceProbability(1), closeTo(0.5 * Math.log(2), 1e-12));
        assertThat(sampler.acceptanceProbability(5), equalTo(1.0));

        clusterSettings.applySettings(
            Settings.builder()
                .put(QuerySamplingSettings.ACCEPTANCE_SCALE.getKey(), 0.25)
                .put(QuerySamplingSettings.HEAD_THRESHOLD.getKey(), 2)
                .build()
        );

        assertThat(sampler.acceptanceProbability(1), closeTo(0.25 * Math.log(2), 1e-12));
        assertThat("the head starts earlier", sampler.acceptanceProbability(2), equalTo(1.0));
    }

    public void testQueriesGetNoChanceOnceThePicksAreUsedUpAndThatIsRecorded() {
        AtomicLong now = new AtomicLong();
        PickBudget budget = new PickBudget(now::get);
        budget.perHour(60); // a bucket of one pick, which comes back after a minute
        QuerySampler sampler = new QuerySampler(1.0, 1000, alwaysDrawing(0.0), budget);
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        TrackedQuery first = tracker.record(new QueryFingerprint(1, 1));
        TrackedQuery second = tracker.record(new QueryFingerprint(2, 2));
        TrackedQuery third = tracker.record(new QueryFingerprint(3, 3));

        assertTrue("the pick there is", sampler.offer(first));
        assertFalse(sampler.offer(second));
        assertFalse(sampler.offer(third));
        assertThat("no chance, which is what the probability of the query says", second.inclusionProbability(), equalTo(0.0));
        assertThat(first.inclusionProbability(), greaterThan(0.0));

        now.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertTrue("a pick is back", sampler.offer(tracker.record(new QueryFingerprint(4, 4))));
    }

    public void testHeadQueriesAreNeverHeldBack() {
        PickBudget budget = new PickBudget(() -> 0L);
        budget.perHour(60);
        QuerySampler sampler = new QuerySampler(1.0, 5, alwaysDrawing(0.999999), budget);
        // the only pick goes to a query that is not a head query, the draw of 0.999999 would leave it
        QuerySampler lucky = new QuerySampler(1.0, 5, alwaysDrawing(0.0), budget);
        assertTrue(lucky.offer(tracked(1)));

        TrackedQuery head = tracked(5);
        assertTrue("the budget is empty, and it is a head query", sampler.offer(head));
        assertThat(head.inclusionProbability(), equalTo(1.0));
    }

    public void testTheLimitFollowsTheSetting() {
        AtomicLong now = new AtomicLong();
        PickBudget budget = new PickBudget(now::get);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.MAX_PICKS_PER_HOUR.getKey(), 60).build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR
            )
        );
        QuerySampler sampler = new QuerySampler(1.0, 1000, alwaysDrawing(0.0), budget);
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        assertTrue(sampler.offer(tracker.record(new QueryFingerprint(1, 1))));
        assertFalse("the limit is one an hour", sampler.offer(tracker.record(new QueryFingerprint(2, 2))));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.MAX_PICKS_PER_HOUR.getKey(), 0).build());
        assertTrue("and then there is none", sampler.offer(tracker.record(new QueryFingerprint(3, 3))));
    }

    public void testTheScaleFollowsTheTargetOfThePicks() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.TARGET_PICKS_PER_HOUR.getKey(), 3600).build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR
            )
        );
        QuerySampler sampler = new QuerySampler(0.5, 1000, alwaysDrawing(0.0));
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(1000);
        assertThat(sampler.effectiveScale(), equalTo(1.0)); // the scale of the setting, which the sampler read

        for (int i = 0; i < 100; i++) {
            assertTrue(sampler.offer(tracker.record(new QueryFingerprint(i, i))));
        }
        sampler.regulate(30); // 100 picks in 30 seconds is 12000 an hour, more than three times the target

        double step = Math.sqrt(3600.0 / 12_000);
        assertThat("lower, by the square root of how far off it was", sampler.effectiveScale(), closeTo(step, 1e-12));
        assertThat(sampler.acceptanceProbability(1), closeTo(step * Math.log(2), 1e-12));
        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.TARGET_PICKS_PER_HOUR.getKey(), 0).build());
        sampler.regulate(1);
        assertThat("and back to the setting when there is no target", sampler.effectiveScale(), equalTo(1.0));
    }

    public void testPicksOfHeadQueriesDoNotCountTowardsTheTarget() {
        QuerySampler sampler = new QuerySampler(0.5, 5, alwaysDrawing(0.0));
        sampler.watch(
            new ClusterSettings(
                Settings.builder()
                    .put(QuerySamplingSettings.TARGET_PICKS_PER_HOUR.getKey(), 3600)
                    .put(QuerySamplingSettings.HEAD_THRESHOLD.getKey(), 5)
                    .build(),
                Set.of(
                    QuerySamplingSettings.ACCEPTANCE_SCALE,
                    QuerySamplingSettings.HEAD_THRESHOLD,
                    QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                    QuerySamplingSettings.TARGET_PICKS_PER_HOUR
                )
            )
        );
        for (int i = 0; i < 100; i++) {
            assertTrue(sampler.offer(tracked(5)));
        }

        sampler.regulate(30);

        assertThat("nothing was picked by chance, so the scale goes up", sampler.effectiveScale(), equalTo(2.0));
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
