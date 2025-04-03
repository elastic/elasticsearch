/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static java.lang.Math.exp;
import static java.lang.Math.expm1;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentiallyWeightedMovingRateTests extends ESTestCase {

    private static final double TOLERANCE = 2.0e-13;
    private static final double HALF_LIFE_MILLIS = 1.0e6; // Half-life of used by many tests
    private static final double LAMBDA = Math.log(2.0) / HALF_LIFE_MILLIS; // Equivalent value of lambda
    public static final int START_TIME_IN_MILLIS = 1234567;

    public void testEwmr() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        // Assert on the rate returned without any increments, either at the start time or a bit later:
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS), equalTo(0.0));
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 900), equalTo(0.0));
        // Do a first increment:
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        // Calculate the expected EWMR at 1000ms after the start time, i.e. the time of the last increment:
        double expected1000 = 10.0 / ((1.0 - exp(-1.0 * LAMBDA * 1000)) / LAMBDA); // increment divided by integral of weights over interval
        // That is 0.010003... (~= 10 / 1000 - greater than that, because an update just happened, and we favour recent values - but only
        // fractionally, because the time interval is a small fraction of the half-life)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 1000), closeTo(expected1000, TOLERANCE));
        // Calculated the expected EWMR at 1900ms after the start time, i.e. 900ms after the time of the last update:
        double expected1900 = 10.0 * exp(-1.0 * LAMBDA * 900) // weighted increment
            / ((1.0 - exp(-1.0 * LAMBDA * 1900)) / LAMBDA); // integral of weights over interval
        // That is 0.005263... (~= 10 / 1900)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 1900), closeTo(expected1900, TOLERANCE));
        // Do two more increments:
        ewmr.addIncrement(12.0, START_TIME_IN_MILLIS + 2000);
        ewmr.addIncrement(8.0, START_TIME_IN_MILLIS + 2500);
        // Calculate the expected weight at 2500ms after the start time, i.e. the time of the last increment:
        double expected2500 = (10.0 * exp(-1.0 * LAMBDA * 1500) // first weighted increment
            + 12.0 * exp(-1.0 * LAMBDA * 500) // second weighted increment
            + 8.0) // third increment, with a weight of 1.0 as no time elapsed
            / ((1.0 - exp(-1.0 * LAMBDA * 2500)) / LAMBDA); // integral of weight function
        // That is 0.012005... (~= 30 / 2500)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2500), closeTo(expected2500, TOLERANCE));
    }

    public void testEwmr_zeroLambda() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(0.0, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(20.0, START_TIME_IN_MILLIS + 1500);
        ewmr.addIncrement(15.0, START_TIME_IN_MILLIS + 2000);
        // Calculate the expected weight at 2000ms after the start time, which is a simple unweighted rate since lambda is zero:
        double expected2000 = (10.0 + 20.0 + 15.0) / 2000; // 0.0225
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2000), closeTo(expected2000, TOLERANCE));
        // Should still get unweighted cumulative rate even if we wait a long time before the next increment:
        ewmr.addIncrement(12.0, START_TIME_IN_MILLIS + 2_000_000);
        double expected2000000 = (10.0 + 20.0 + 15.0 + 12.0) / 2_000_000; // 0.0000285
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2_000_000), closeTo(expected2000000, TOLERANCE));
    }

    public void testEwmr_longSeriesEvenRate() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        // Do loads of updates of size effectiveRate * intervalMillis ever intervalMillis:
        long intervalMillis = 5000;
        int numIncrements = 100_000;
        double effectiveRate = 0.123;
        for (int i = 1; i <= numIncrements; i++) {
            ewmr.addIncrement(effectiveRate * intervalMillis, START_TIME_IN_MILLIS + intervalMillis * i);
        }
        // Expected rate is roughly effectiveRate - use wider tolerance here as we this is an approximation:
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + intervalMillis * numIncrements), closeTo(effectiveRate, 0.001));
    }

    public void testEwmr_longSeriesWithStepChangeInRate_fitsHalfLife_contrastWithZeroLambda() {
        // In this test, we use a standard Exponentially Weighted Moving Rate...:
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        // ...and also one with zero lambda, giving an unweighted Cumulative Moving Rate:
        ExponentiallyWeightedMovingRate cmr = new ExponentiallyWeightedMovingRate(0.0, START_TIME_IN_MILLIS);
        long intervalMillis = 1000;

        // Phase 1: numIncrements1 increments at effective rate effectiveRate1 (increment size effectiveRate1 * intervalMillis):
        int numIncrements1 = 90_000; // 90_000 increments at intervals of 1000ms take 100_000_000ms, or 90 half-lives
        double effectiveRate1 = 0.123;
        for (int i = 1; i <= numIncrements1; i++) {
            ewmr.addIncrement(effectiveRate1 * intervalMillis, START_TIME_IN_MILLIS + intervalMillis * i);
            cmr.addIncrement(effectiveRate1 * intervalMillis, START_TIME_IN_MILLIS + intervalMillis * i);
        }
        // Expected rate for both EWMR and CMR is roughly effectiveRate1 - use wider tolerance here as we this is an approximation:
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + intervalMillis * numIncrements1), closeTo(effectiveRate1, 0.001));
        assertThat(cmr.getRate(START_TIME_IN_MILLIS + intervalMillis * numIncrements1), closeTo(effectiveRate1, 0.001));

        // Phase 2a: numIncrements2a increments at effective rate effectiveRate2 (increment size effectiveRate2 * intervalMillis):
        long phase2aStartTimeMillis = START_TIME_IN_MILLIS + intervalMillis * numIncrements1;
        int numIncrements2a = 1000; // 1000 increments at intervals of 1000ms take 1_000_000ms, or exactly one half-life
        double effectiveRate2 = 0.345;
        // Do the first increment at the higher rate:
        ewmr.addIncrement(effectiveRate2 * intervalMillis, phase2aStartTimeMillis + intervalMillis);
        cmr.addIncrement(effectiveRate2 * intervalMillis, phase2aStartTimeMillis + intervalMillis);
        // That first increment at the higher rate shouldn't make much difference to the EWMR, we still expect roughly effectiveRate1:
        assertThat(ewmr.getRate(phase2aStartTimeMillis + intervalMillis), closeTo(effectiveRate1, 0.001));
        // Now do the rest of the 1000 increments:
        for (int i = 2; i <= numIncrements2a; i++) {
            ewmr.addIncrement(effectiveRate2 * intervalMillis, phase2aStartTimeMillis + intervalMillis * i);
            cmr.addIncrement(effectiveRate2 * intervalMillis, phase2aStartTimeMillis + intervalMillis * i);
        }
        // Since we have been at effectiveRate2 for one half-life, and we were at effectiveRate1 for 90 half-lives (which is effectively
        // forever) before that, we expect the EWMR to be roughly halfway between the two rates:
        double expectedEwmr2a = 0.5 * (effectiveRate1 + effectiveRate2); // 0.5 * (0.123 + 0.345) = 0.234
        assertThat(ewmr.getRate(phase2aStartTimeMillis + intervalMillis * numIncrements2a), closeTo(expectedEwmr2a, 0.001));

        // Phase 2b: numIncrements2b increments at effective rate effectiveRate2 (increment size effectiveRate2 * intervalMillis):
        long phase2bStartTimeMillis = phase2aStartTimeMillis + intervalMillis * numIncrements2a;
        int numIncrements2b = 9000; // 9000 increments at intervals of 1000ms take 9_000_000ms, or 9 half-lives
        for (int i = 1; i <= numIncrements2b; i++) {
            ewmr.addIncrement(effectiveRate2 * intervalMillis, phase2bStartTimeMillis + intervalMillis * i);
            cmr.addIncrement(effectiveRate2 * intervalMillis, phase2bStartTimeMillis + intervalMillis * i);
        }
        // Since we have been at effectiveRate2 for 10 half-lives (which is effectively forever) across phases 2a and 2b, that's now the
        // approximate expected EWMR:
        assertThat(ewmr.getRate(phase2bStartTimeMillis + intervalMillis * numIncrements2b), closeTo(effectiveRate2, 0.001));
        // We did 90_000 increments at effectiveRate1 and 10_000 at effectiveRate2, and for the CMR each increment is equally weighted, so
        // we expect the CMR to place 9x the weight on effectiveRate1 relative to effectiveRate2:
        double expectedCmr2b = 0.9 * effectiveRate1 + 0.1 * effectiveRate2; // 0.9 * 0.123 + 0.1 * 0.345 = 0.1452
        assertThat(cmr.getRate(phase2bStartTimeMillis + intervalMillis * numIncrements2b), closeTo(expectedCmr2b, 0.001));
        // N.B. At this point the EWMR is dominated by the new rate, while the CMR is still largely dominated by the old rate.
    }

    public void testEwmr_longGapBetweenValues_higherRecentValue() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(30.0, START_TIME_IN_MILLIS + 2_000_000);
        double expected2000000 = (10.0 * exp(-1.0 * LAMBDA * 1_999_000) + 30.0) / ((1.0 - exp(-1.0 * LAMBDA * 2_000_000)) / LAMBDA);
        // 0.000030... (more than 40 / 2000000 = 0.000020 because the gap is twice the half-life and we favour the larger recent increment)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2_000_000), closeTo(expected2000000, TOLERANCE));
    }

    public void testEwmr_longGapBetweenValues_lowerRecentValue() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(30.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 2_000_000);
        double expected2000000 = (30.0 * exp(-1.0 * LAMBDA * 1_999_000) + 10.0) / ((1.0 - exp(-1.0 * LAMBDA * 2_000_000)) / LAMBDA);
        // 0.000016... (less than 40 / 2000000 = 0.000020 because the gap is twice the half-life and we favour the smaller recent increment)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2_000_000), closeTo(expected2000000, TOLERANCE));
    }

    public void testEwmr_longGapBeforeFirstValue() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 2_000_000);
        double expected2000000 = 10.0 / ((1.0 - exp(-1.0 * LAMBDA * 2_000_000)) / LAMBDA);
        // 0.000009... (more than 10 / 2000000 = 0.000005 because the gap twice the half-life and we favour the recent increment)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2_000_000), closeTo(expected2000000, TOLERANCE));
    }

    public void testEwmr_longGapAfterLastIncrement() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(12.0, START_TIME_IN_MILLIS + 2000);
        ewmr.addIncrement(8.0, START_TIME_IN_MILLIS + 2500);
        double expected2000000 = (10.0 * exp(-1.0 * LAMBDA * 1_999_000) // first weighted increment
            + 12.0 * exp(-1.0 * LAMBDA * 1_998_000) // second weighted increment
            + 8.0 * exp(-1.0 * LAMBDA * 1_997_500)) // third weighted increment
            / ((1.0 - exp(-1.0 * LAMBDA * 2_000_000)) / LAMBDA); // integral of weights over interval
        // 0.000007... (less than 30 / 2000000 = 0.000015 because the updates were nearly two half-lives ago)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2_000_000), closeTo(expected2000000, TOLERANCE));
    }

    public void testEwmr_firstIncrementHappensImmediately() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS);
        // The method contract states that we treat this as if the increment time was START_TIME_IN_MILLIS + 1:
        // N.B. We have to use expm1 here when calculating the expected value to avoid floating point error from 1.0 - exp(-1.0e-6).
        double expected = 10.0 / (-1.0 * expm1(-1.0 * LAMBDA * 1) / LAMBDA); // 10.000003... (~= 10 / 1)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS), closeTo(expected, TOLERANCE));
    }

    public void testEwmr_timeFlowsBackwardsBetweenIncrements() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(20.0, START_TIME_IN_MILLIS + 900);
        // The method contract states that we treat this as if both increments happened at START_TIME_IN_MILLIS + 1000:
        double expected900 = (10.0 + 20.0) / ((1.0 - exp(-1.0 * LAMBDA * 1000)) / LAMBDA); // 0.030010 (~= 30 / 1000)
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 900), closeTo(expected900, TOLERANCE));
    }

    public void testEwmr_askForRateAtTimeBeforeLastIncrement() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 1000);
        ewmr.addIncrement(12.0, START_TIME_IN_MILLIS + 2000);
        ewmr.addIncrement(8.0, START_TIME_IN_MILLIS + 2500);
        double expected2500 = (10.0 * exp(-1.0 * LAMBDA * 1500) // first weighted increment
            + 12.0 * exp(-1.0 * LAMBDA * 500) // second weighted increment
            + 8.0) // third increment
            / ((1.0 - exp(-1.0 * LAMBDA * 2500)) / LAMBDA); // integral of weights over interval
        // 0.012005... (~= 30 / 2500)
        // The method contract states that, if we ask for the rate at a time before the last increment, we get the rate at the time of the
        // last increment instead:
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + 2400), closeTo(expected2500, TOLERANCE));
    }

    public void testEwmr_negativeLambdaThrowsOnConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new ExponentiallyWeightedMovingRate(-1.0e-6, START_TIME_IN_MILLIS));
    }

    // N.B. This test is not guaranteed to fail even if the implementation is not thread-safe. The operations are fast enough that there is
    // a chance each thread will complete before the next one has started. We use a high thread count to try to get a decent change of
    // hitting a race condition if there is one. This should be run with e.g. -Dtests.iters=20 to test thoroughly.
    public void testEwmr_threadSafe() throws InterruptedException {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        int numRoundsOfIncrements = 100; // We will do this many rounds of increments
        long intervalMillis = 10_000; // This is the interval between each round of increments
        int numThreads = 1000; // In each round, we will do this many concurrent updates, each on its own thread, all at the same timestamp
        List<Double> totalIncrementsPerRound = new ArrayList<>(); // We will store the total increment for each round in this list
        for (int round = 1; round <= numRoundsOfIncrements; round++) {
            long timeInMillis = START_TIME_IN_MILLIS + round * intervalMillis;
            double[] incrementsForRound = DoubleStream.generate(() -> randomDoubleBetween(1.0, 100.0, true)).limit(numThreads).toArray();
            List<Thread> threads = DoubleStream.of(incrementsForRound)
                .mapToObj(increment -> new Thread(() -> ewmr.addIncrement(increment, timeInMillis)))
                .toList();
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            totalIncrementsPerRound.add(DoubleStream.of(incrementsForRound).sum());
        }
        double expectedEwmr = IntStream.range(0, numRoundsOfIncrements)
            .mapToDouble(i -> totalIncrementsPerRound.get(i) * exp(-1.0 * LAMBDA * (numRoundsOfIncrements - 1 - i) * intervalMillis))
            .sum() // sum of weighted increments
            / ((1.0 - exp(-1.0 * LAMBDA * numRoundsOfIncrements * intervalMillis)) / LAMBDA); // integral of weights over interval
        assertThat(ewmr.getRate(START_TIME_IN_MILLIS + numRoundsOfIncrements * intervalMillis), closeTo(expectedEwmr, TOLERANCE));
    }

    public void testCalculateRateSince() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);

        // Do a first batch of updates, then get and assert on the EWMR calculated for them some time after the last of them:
        ewmr.addIncrement(80.0, START_TIME_IN_MILLIS + 100_000);
        ewmr.addIncrement(90.0, START_TIME_IN_MILLIS + 200_000);
        ewmr.addIncrement(70.0, START_TIME_IN_MILLIS + 300_000);
        double expectedFirstBatch = (90.0 * exp(-1.0 * LAMBDA * 200_000) // first weighted increment
            + 80.0 * exp(-1.0 * LAMBDA * 300_000) // second weighted increment
            + 70.0 * exp(-1.0 * LAMBDA * 100_000)) // third weighted increment
            / ((1.0 - exp(-1.0 * LAMBDA * 400_000)) / LAMBDA); // integral of weights over interval
        // That is 0.00059...
        double ewmrFirstBatch = ewmr.getRate(START_TIME_IN_MILLIS + 400_000);
        assertThat(ewmrFirstBatch, closeTo(expectedFirstBatch, TOLERANCE));

        // Do a second batch of updates, then get and assert on the EWMR calculated for them some time after the last of them (including
        // all the updates since the start time, i.e. the updates from both batches):
        ewmr.addIncrement(20.0, START_TIME_IN_MILLIS + 500_000);
        ewmr.addIncrement(30.0, START_TIME_IN_MILLIS + 600_000);
        ewmr.addIncrement(10.0, START_TIME_IN_MILLIS + 700_000);
        ewmr.addIncrement(40.0, START_TIME_IN_MILLIS + 800_000);
        double expectedBothBatches = (80.0 * exp(-1.0 * LAMBDA * 800_000) // first weighted increment from first batch
            + 90.0 * exp(-1.0 * LAMBDA * 700_000) // second weighted increment from first batch
            + 70.0 * exp(-1.0 * LAMBDA * 600_000) // third weighted increment from first batch
            + 20.0 * exp(-1.0 * LAMBDA * 400_000) // first weighted increment from second batch
            + 30.0 * exp(-1.0 * LAMBDA * 300_000) // second weighted increment from second batch
            + 10.0 * exp(-1.0 * LAMBDA * 200_000) // third weighted increment from second batch
            + 40.0 * exp(-1.0 * LAMBDA * 100_000))  // fourth weighted increment from second batch
            / ((1.0 - exp(-1.0 * LAMBDA * 900_000)) / LAMBDA); // integral of weights over interval
        // That is 0.00034...
        double ewmrBothBatches = ewmr.getRate(START_TIME_IN_MILLIS + 900_000);
        assertThat(ewmrBothBatches, closeTo(expectedBothBatches, TOLERANCE));

        // Get and assert on the EWMR calculated as if the calculation started at the point we got the rate after the first batch of
        // increments. Note that the expected value depends only on the second batch of increments.
        double expectedSecondBatchOnly = (20.0 * exp(-1.0 * LAMBDA * 400_000) // first weighted increment from second batch
            + 30.0 * exp(-1.0 * LAMBDA * 300_000) // second weighted increment from second batch
            + 10.0 * exp(-1.0 * LAMBDA * 200_000) // third weighted increment from second batch
            + 40.0 * exp(-1.0 * LAMBDA * 100_000))  // fourth weighted increment from second batch
            / ((1.0 - exp(-1.0 * LAMBDA * 500_000)) / LAMBDA); // integral of weights over interval
        // That is 0.00020...
        double calculatedSecondBatchOnly = ewmr.calculateRateSince(
            START_TIME_IN_MILLIS + 900_000,
            ewmrBothBatches,
            START_TIME_IN_MILLIS + 400_000,
            ewmrFirstBatch
        );
        assertThat(calculatedSecondBatchOnly, closeTo(expectedSecondBatchOnly, TOLERANCE));

        // Double check this result by directly calculating the EWMR with the later start time and the second batch of updates.
        ExponentiallyWeightedMovingRate ewmr2 = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS + 400_000);
        ewmr2.addIncrement(20.0, START_TIME_IN_MILLIS + 500_000);
        ewmr2.addIncrement(30.0, START_TIME_IN_MILLIS + 600_000);
        ewmr2.addIncrement(10.0, START_TIME_IN_MILLIS + 700_000);
        ewmr2.addIncrement(40.0, START_TIME_IN_MILLIS + 800_000);
        double directEwmrSecondBatchOnly = ewmr2.getRate(START_TIME_IN_MILLIS + 900_000);
        assertThat(calculatedSecondBatchOnly, closeTo(directEwmrSecondBatchOnly, TOLERANCE));
    }

    public void testCalculateRateSince_noMoreIncrements() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);

        // Do a batch of updates, then get and assert on the EWMR calculated for them some time after the last of them:
        ewmr.addIncrement(80.0, START_TIME_IN_MILLIS + 100_000);
        ewmr.addIncrement(90.0, START_TIME_IN_MILLIS + 200_000);
        ewmr.addIncrement(70.0, START_TIME_IN_MILLIS + 300_000);
        double expected400k = (80.0 * exp(-1.0 * LAMBDA * 300_000) // first weighted increment
            + 90.0 * exp(-1.0 * LAMBDA * 200_000) // second weighted increment
            + 70.0 * exp(-1.0 * LAMBDA * 100_000)) // third weighted increment
            / ((1.0 - exp(-1.0 * LAMBDA * 400_000)) / LAMBDA); // integral of weights over interval
        // That is 0.00059...
        double ewmr400k = ewmr.getRate(START_TIME_IN_MILLIS + 400_000);
        assertThat(ewmr400k, closeTo(expected400k, TOLERANCE));

        // Without doing any more updates, get and assert on the EWMR calculated for a later time:
        double expected500k = (80.0 * exp(-1.0 * LAMBDA * 400_000) // first weighted increment
            + 90.0 * exp(-1.0 * LAMBDA * 300_000) // second weighted increment
            + 70.0 * exp(-1.0 * LAMBDA * 200_000)) // third weighted increment
            / ((1.0 - exp(-1.0 * LAMBDA * 500_000)) / LAMBDA); // integral of weights over interval
        // That is 0.00045...
        double ewmr500k = ewmr.getRate(START_TIME_IN_MILLIS + 500_000);
        assertThat(ewmr500k, closeTo(expected500k, TOLERANCE));

        // Get and assert on the EWMR for the interval between the two rates we got, which should be zero are there are no updates:
        assertThat(
            ewmr.calculateRateSince(START_TIME_IN_MILLIS + 500_000, ewmr500k, START_TIME_IN_MILLIS + 400_000, ewmr400k),
            closeTo(0.0, TOLERANCE) // still need an approximate check as floating point errors mean it won't be exactly zero
        );
    }

    public void testCalculateRateSince_currentTimeNotAfterOldTime() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        // The method contract says calculateRateSince should return 0.0 if currentTimeMillis is the same as or earlier than oldTimeMillis.
        // This is the case whether the rates are the same or different.
        assertThat(ewmr.calculateRateSince(START_TIME_IN_MILLIS + 1000, 123.0, START_TIME_IN_MILLIS + 1000, 123.0), equalTo(0.0));
        assertThat(ewmr.calculateRateSince(START_TIME_IN_MILLIS + 1000, 123.0, START_TIME_IN_MILLIS + 1000, 456), equalTo(0.0));
        assertThat(ewmr.calculateRateSince(START_TIME_IN_MILLIS + 500, 123.0, START_TIME_IN_MILLIS + 1000, 123.0), equalTo(0.0));
        assertThat(ewmr.calculateRateSince(START_TIME_IN_MILLIS + 500, 123.0, START_TIME_IN_MILLIS + 1000, 456), equalTo(0.0));
    }

    public void testGetHalfLife() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(LAMBDA, START_TIME_IN_MILLIS);
        assertThat(ewmr.getHalfLife(), closeTo(HALF_LIFE_MILLIS, 1.0e-9));
    }

    public void testGetHalfLife_lambdaZero() {
        ExponentiallyWeightedMovingRate ewmr = new ExponentiallyWeightedMovingRate(0.0, START_TIME_IN_MILLIS);
        assertThat(ewmr.getHalfLife(), equalTo(Double.POSITIVE_INFINITY));
    }
}
