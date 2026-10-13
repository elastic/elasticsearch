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
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
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
import static org.hamcrest.Matchers.not;

public class SamplerTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1, 1);

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
        assertThat(total, closeTo(scale * Math.log(1001), 1e-9));
    }

    public void testWeightedArrivalsAddUpToTheSameAsUnitOnes() {
        double scale = 0.2;
        QuerySampler sampler = new QuerySampler(scale, Long.MAX_VALUE, seededRandom());
        double weight = 10;
        double total = 0;
        double estimatedArrivals = 0;
        for (int arrival = 0; arrival < 100; arrival++) {
            estimatedArrivals += weight;
            total += sampler.acceptanceProbability(estimatedArrivals, weight);
        }
        assertThat(total, closeTo(scale * Math.log(1 + estimatedArrivals), 1e-9));
    }

    public void testQueriesGetNoChanceOnceThePicksAreUsedUpAndThatIsRecorded() {
        AtomicLong now = new AtomicLong();
        PickBudget budget = new PickBudget(now::get);
        budget.perHour(60);
        QuerySampler sampler = new QuerySampler(1.0, 1000, alwaysDrawing(0.0), budget);
        MultiplicityTracker tracker = new MultiplicityTracker(10);

        TrackedQuery first = tracker.record(new QueryFingerprint(1, 1));
        TrackedQuery second = tracker.record(new QueryFingerprint(2, 2));
        TrackedQuery third = tracker.record(new QueryFingerprint(3, 3));

        assertTrue("the pick there is", sampler.offer(first));
        assertFalse(sampler.offer(second));
        assertFalse(sampler.offer(third));
        assertThat("no chance, which is what the probability of the query says", second.inclusionProbability(), equalTo(0.0));
        assertThat("both were turned away by the limit", sampler.starved(), equalTo(2L));
        assertThat(first.inclusionProbability(), greaterThan(0.0));

        now.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertTrue("a pick is back", sampler.offer(tracker.record(new QueryFingerprint(4, 4))));
    }

    public void testHeadQueriesAreNeverHeldBack() {
        PickBudget budget = new PickBudget(() -> 0L);
        budget.perHour(60);
        QuerySampler sampler = new QuerySampler(1.0, 5, alwaysDrawing(0.999999), budget);
        QuerySampler lucky = new QuerySampler(1.0, 5, alwaysDrawing(0.0), budget);
        assertTrue(lucky.offer(tracked(1)));

        TrackedQuery head = tracked(5);
        assertTrue("the budget is empty, and it is a head query", sampler.offer(head));
        assertThat(head.inclusionProbability(), equalTo(1.0));
    }

    public void testTheScaleFollowsTheTargetOfThePicks() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.TARGET_PICKS_PER_HOUR.getKey(), 3600).build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR,
                QuerySamplingSettings.SPATIAL_BALANCE,
                QuerySamplingSettings.HARDNESS_TILT
            )
        );
        QuerySampler sampler = new QuerySampler(0.5, 1000, alwaysDrawing(0.0));
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(1000);
        assertThat(sampler.effectiveScale(), equalTo(1.0));

        for (int i = 0; i < 100; i++) {
            assertTrue(sampler.offer(tracker.record(new QueryFingerprint(i, i))));
        }
        sampler.regulate(30);

        double step = Math.sqrt(3600.0 / 12_000);
        assertThat("lower, by the square root of how far off it was", sampler.effectiveScale(), closeTo(step, 1e-12));
        assertThat(sampler.acceptanceProbability(1), closeTo(step * Math.log(2), 1e-12));
        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.TARGET_PICKS_PER_HOUR.getKey(), 0).build());
        sampler.regulate(1);
        assertThat("and back to the setting when there is no target", sampler.effectiveScale(), equalTo(1.0));
    }

    public void testQueriesOfSparseClustersAreFavouredWhenBalancing() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), 1.0)
                .put(QuerySamplingSettings.ACCEPTANCE_SCALE.getKey(), 0.1)
                .build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR,
                QuerySamplingSettings.SPATIAL_BALANCE,
                QuerySamplingSettings.HARDNESS_TILT
            )
        );
        QuerySampler sampler = new QuerySampler(
            0.1,
            1000,
            alwaysDrawing(0.999999),
            new PickBudget(System::nanoTime),
            new SpatialStrata(2, 2, () -> new Random(3L))
        );
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(1000);
        TrackedQuery first = tracker.record(new QueryFingerprint(0, 0));
        TrackedQuery dense = first;
        sampler.assignStratum(first, "vector", new float[] { 0, 0 });
        TrackedQuery sparse = tracker.record(new QueryFingerprint(100, 100));
        sampler.assignStratum(sparse, "vector", new float[] { 10, 10 });
        for (int i = 1; i < 9; i++) {
            dense = tracker.record(new QueryFingerprint(i, i));
            sampler.assignStratum(dense, "vector", new float[] { 0.01f * i, 0 });
        }
        assertThat("the others join the first", dense.stratum(), equalTo(first.stratum()));
        assertThat(sparse.stratum(), not(equalTo(dense.stratum())));

        sampler.offer(dense);
        sampler.offer(sparse);

        double plain = sampler.acceptanceProbability(1);
        assertThat(dense.inclusionProbability(), lessThan(plain));
        assertThat("the factor is recorded in what the draw had", sparse.inclusionProbability(), greaterThan(plain));
        assertThat(dense.inclusionProbability() / sparse.inclusionProbability(), lessThan(0.2));
    }

    public void testHardQueriesAreFavouredAndEasyOnesAreNotWhenTilting() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(QuerySamplingSettings.HARDNESS_TILT.getKey(), 1.0)
                .put(QuerySamplingSettings.ACCEPTANCE_SCALE.getKey(), 0.1)
                .build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR,
                QuerySamplingSettings.SPATIAL_BALANCE,
                QuerySamplingSettings.HARDNESS_TILT
            )
        );
        QuerySampler sampler = new QuerySampler(1.0, 1000, alwaysDrawing(0.999999));
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        TrackedQuery hard = tracker.record(new QueryFingerprint(1, 1));
        hard.hardness(Hardness.HARD);
        TrackedQuery easy = tracker.record(new QueryFingerprint(2, 2));
        easy.hardness(Hardness.EASY);
        TrackedQuery unknown = tracker.record(new QueryFingerprint(3, 3));

        sampler.offer(hard);
        sampler.offer(easy);
        sampler.offer(unknown);

        double plain = sampler.acceptanceProbability(1);
        assertThat("nothing is known of it", unknown.inclusionProbability(), closeTo(plain, 1e-12));
        assertThat("the factor is recorded in what the draw had", hard.inclusionProbability(), greaterThan(plain));
        assertThat(easy.inclusionProbability(), lessThan(plain));
        assertThat(hard.inclusionProbability() / easy.inclusionProbability(), closeTo(Math.exp(2.0), 1e-9));
    }

    public void testAHighScaleMakesUpForTheFactorOfACrowdedCluster() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), 1.0)
                .put(QuerySamplingSettings.ACCEPTANCE_SCALE.getKey(), 10.0)
                .build(),
            Set.of(
                QuerySamplingSettings.ACCEPTANCE_SCALE,
                QuerySamplingSettings.HEAD_THRESHOLD,
                QuerySamplingSettings.MAX_PICKS_PER_HOUR,
                QuerySamplingSettings.TARGET_PICKS_PER_HOUR,
                QuerySamplingSettings.SPATIAL_BALANCE,
                QuerySamplingSettings.HARDNESS_TILT
            )
        );
        QuerySampler sampler = new QuerySampler(
            1.0,
            1000,
            alwaysDrawing(0.999999),
            new PickBudget(System::nanoTime),
            new SpatialStrata(2, 2, () -> new Random(3L))
        );
        sampler.watch(clusterSettings);
        MultiplicityTracker tracker = new MultiplicityTracker(1000);
        TrackedQuery dense = tracker.record(new QueryFingerprint(0, 0));
        sampler.assignStratum(dense, "vector", new float[] { 0, 0 });
        sampler.assignStratum(tracker.record(new QueryFingerprint(100, 100)), "vector", new float[] { 10, 10 });
        for (int i = 1; i < 9; i++) {
            dense = tracker.record(new QueryFingerprint(i, i));
            sampler.assignStratum(dense, "vector", new float[] { 0.01f * i, 0 });
        }

        sampler.offer(dense);

        assertThat(dense.inclusionProbability(), equalTo(1.0));
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

    private final AtomicLong now = new AtomicLong();

    private final PickBudget budget = new PickBudget(now::get);

    public void testTheBucketStartsFullAndIsEmptiedByPicks() {
        budget.perHour(3600);

        for (int i = 0; i < 60; i++) {
            assertTrue("pick " + i, budget.available());
            budget.take();
        }
        assertFalse(budget.available());
    }

    public void testTokensComeBackAtTheRateOfTheLimit() {
        budget.perHour(3600);
        for (int i = 0; i < 60; i++) {
            budget.take();
        }
        assertFalse(budget.available());

        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(1500));
        assertTrue("one token after a second and a half", budget.available());
        budget.take();
        assertFalse("and not two", budget.available());
    }

    public void testAQuietSpellDoesNotStoreUpMoreThanABurst() {
        budget.perHour(3600);

        now.addAndGet(TimeUnit.HOURS.toNanos(1));
        int picks = 0;
        while (budget.available() && picks < 1000) {
            budget.take();
            picks++;
        }

        assertThat(picks, equalTo(60));
    }

    public void testChangingTheLimitKeepsWhatIsInTheBucket() {
        budget.perHour(3600);
        for (int i = 0; i < 60; i++) {
            budget.take();
        }

        budget.perHour(7200);
        assertFalse("raising the limit does not refill the bucket", budget.available());

        budget.perHour(0);
        assertTrue("removing it lifts it", budget.available());

        budget.perHour(3600);
        assertTrue("a limit that is set again starts with a full bucket", budget.available());
    }

    private final ScaleRegulator regulator = new ScaleRegulator();

    private void pick(int picks) {
        for (int i = 0; i < picks; i++) {
            regulator.recordPick();
        }
    }

    public void testWaitsForAWindowBeforeLookingAtTheRate() {
        regulator.targetPerHour(3600);
        pick(1000);

        regulator.observe(10);
        regulator.observe(10);
        assertThat("less than a window", regulator.adjustment(), equalTo(1.0));

        regulator.observe(10);
        assertThat("a window", regulator.adjustment(), equalTo(0.5));
    }

    public void testPicksTooFastBringsTheScaleDown() {
        regulator.targetPerHour(3600);
        pick(60);

        regulator.observe(30);

        assertThat(regulator.adjustment(), closeTo(Math.sqrt(0.5), 1e-12));
    }

    public void testPicksTooSlowlyRaisesTheScale() {
        regulator.targetPerHour(3600);
        pick(10);

        regulator.observe(30);

        assertThat(regulator.adjustment(), closeTo(Math.sqrt(3.0), 1e-12));
    }

    public void testASingleStepIsNeverMoreThanAFactorOfTwo() {
        regulator.targetPerHour(3600);
        regulator.observe(30);
        assertThat("no picks at all", regulator.adjustment(), equalTo(2.0));

        pick(100_000);
        regulator.observe(30);
        assertThat("far too many", regulator.adjustment(), equalTo(1.0));
    }

    public void testTheAdjustmentIsBounded() {
        regulator.targetPerHour(3600);
        for (int i = 0; i < 100; i++) {
            regulator.observe(30);
        }
        assertThat(regulator.adjustment(), equalTo(ScaleRegulator.MAX_ADJUSTMENT));

        pick(10_000_000);
        for (int i = 0; i < 100; i++) {
            regulator.observe(30);
            pick(10_000_000);
        }
        assertThat(regulator.adjustment(), equalTo(ScaleRegulator.MIN_ADJUSTMENT));
    }

    public void testSettlesAtATargetAndStaysThere() {
        regulator.targetPerHour(3600);
        double picksPerWindowAtFullScale = 120;
        for (int window = 0; window < 20; window++) {
            pick((int) Math.round(picksPerWindowAtFullScale * regulator.adjustment()));
            regulator.observe(30);
        }

        assertThat("30 seconds of 3600 picks an hour", picksPerWindowAtFullScale * regulator.adjustment(), closeTo(30, 1.5));
    }

    private static EventSlice slice(double rate, Random random) {
        EventSlice slice = new EventSlice(() -> random);
        slice.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), rate).build(),
                Set.of(QuerySamplingSettings.EVENT_SLICE_RATE)
            )
        );
        return slice;
    }

    public void testAKeptSearchCarriesTheProbabilityItHadOfBeingKept() {
        EventSlice slice = slice(0.25, new Random(randomLong()) {
            @Override
            public double nextDouble() {
                return 0.2;
            }
        });

        assertThat(slice.draw(), equalTo(0.25));
    }

    public void testAboutTheRateOfSearchesIsKept() {
        EventSlice slice = slice(0.1, new Random(randomLong()));
        int trials = 20_000;
        int kept = 0;
        for (int i = 0; i < trials; i++) {
            kept += slice.draw() > 0 ? 1 : 0;
        }

        assertThat((double) kept / trials, closeTo(0.1, 5 * Math.sqrt(0.1 * 0.9 / trials)));
    }

    public void testRateFollowsTheSettingWhenItChanges() {
        EventSlice slice = new EventSlice(() -> new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.EVENT_SLICE_RATE));
        slice.watch(clusterSettings);
        assertThat(slice.draw(), equalTo(0.0));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), 0.5).build());

        assertThat(slice.draw(), equalTo(0.5));
    }
}
