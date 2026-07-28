/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class WelfordVarianceTests extends ESTestCase {

    public void testEmptyReturnsZeroMeanAndVariance() {
        WelfordVariance stats = new WelfordVariance();
        assertThat(stats.mean(), closeTo(0.0, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(0.0, 1e-10));
        assertThat(stats.populationVariance(), closeTo(0.0, 1e-10));
    }

    public void testSingleValueHasZeroVariance() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(7.5);
        assertThat(stats.mean(), closeTo(7.5, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(0.0, 1e-10));
        assertThat(stats.populationVariance(), closeTo(0.0, 1e-10));
    }

    public void testMeanAndSampleVarianceMatchKnownValues() {
        double[] values = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        WelfordVariance stats = new WelfordVariance();
        for (double value : values) {
            stats.add(value);
        }
        assertThat(stats.mean(), closeTo(3.0, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(2.5, 1e-10));
        assertThat(stats.populationVariance(), closeTo(2.0, 1e-10));
    }

    public void testIncrementalUpdatesMatchBatchComputation() {
        double[] values = new double[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 0.25;
        }
        WelfordVariance stats = new WelfordVariance();
        for (double value : values) {
            stats.add(value);
        }
        double batchMean = 0.0;
        for (double value : values) {
            batchMean += value;
        }
        batchMean /= values.length;
        double batchSampleVar = 0.0;
        for (double value : values) {
            double delta = value - batchMean;
            batchSampleVar += delta * delta;
        }
        batchSampleVar /= values.length - 1;
        assertThat(stats.mean(), closeTo(batchMean, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(batchSampleVar, 1e-10));
    }

    public void testAddIntWidensToDouble() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(2);
        stats.add(4);
        assertThat(stats.mean(), closeTo(3.0, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(2.0, 1e-10));
    }

    public void testMergeCombinesIndependentAccumulators() {
        WelfordVariance left = new WelfordVariance();
        left.add(1.0);
        left.add(3.0);
        WelfordVariance right = new WelfordVariance();
        right.add(5.0);
        right.add(7.0);
        WelfordVariance merged = new WelfordVariance();
        for (double value : new double[] { 1.0, 3.0, 5.0, 7.0 }) {
            merged.add(value);
        }
        WelfordVariance combined = new WelfordVariance();
        combined.merge(left);
        combined.merge(right);
        assertThat(combined.mean(), closeTo(merged.mean(), 1e-10));
        assertThat(combined.sampleVariance(), closeTo(merged.sampleVariance(), 1e-10));
    }

    public void testMergeNoOpWhenOtherIsEmpty() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(1.0);
        stats.add(2.0);
        WelfordVariance empty = new WelfordVariance();
        stats.merge(empty);
        assertThat(stats.mean(), closeTo(1.5, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(0.5, 1e-10));
    }

    public void testPopulationStdDev() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(2.0);
        stats.add(4.0);
        stats.add(4.0);
        stats.add(4.0);
        stats.add(5.0);
        stats.add(5.0);
        stats.add(7.0);
        stats.add(9.0);
        assertThat(stats.populationStdDev(), closeTo(2.0, 1e-10));
    }

    public void testAddAsObservationSkipsCountedObservationsWithoutValues() {
        WelfordVariance stats = new WelfordVariance();
        stats.advanceToObservation(1);
        stats.addAsObservation(3.0, 2);
        stats.advanceToObservation(3);
        stats.addAsObservation(5.0, 4);

        WelfordVariance reference = new WelfordVariance();
        int observationNumber = 0;
        for (Double value : new Double[] { null, 3.0, null, 5.0 }) {
            observationNumber += 1;
            if (value == null) {
                reference.advanceToObservation(observationNumber);
                continue;
            }
            reference.addAsObservation(value, observationNumber);
        }
        assertThat(stats.mean(), closeTo(reference.mean(), 1e-10));
        assertThat(stats.m2(), closeTo(reference.m2(), 1e-10));
    }

    public void testCountTracksAddCalls() {
        WelfordVariance stats = new WelfordVariance();
        assertEquals(0, stats.count());
        stats.add(1.0);
        assertEquals(1, stats.count());
        stats.add(2.0);
        stats.add(3.0);
        assertEquals(3, stats.count());
    }

    public void testSampleStdDev() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(2.0);
        stats.add(4.0);
        stats.add(4.0);
        stats.add(4.0);
        stats.add(5.0);
        stats.add(5.0);
        stats.add(7.0);
        stats.add(9.0);
        assertThat(stats.sampleStdDev(), closeTo(Math.sqrt(4.571428571428571), 1e-10));
    }

    public void testConstantValuesHaveZeroVariance() {
        WelfordVariance stats = new WelfordVariance();
        for (int i = 0; i < 10; i++) {
            stats.add(42.0);
        }
        assertThat(stats.mean(), closeTo(42.0, 1e-10));
        assertThat(stats.m2(), closeTo(0.0, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(0.0, 1e-10));
        assertThat(stats.populationVariance(), closeTo(0.0, 1e-10));
    }

    public void testNegativeValues() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(-3.0);
        stats.add(-1.0);
        stats.add(1.0);
        stats.add(3.0);
        assertThat(stats.mean(), closeTo(0.0, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(20.0 / 3.0, 1e-10));
    }

    public void testLargeMeanWithSmallSpread() {
        double base = 1.0e9;
        WelfordVariance stats = new WelfordVariance();
        stats.add(base);
        stats.add(base + 1.0);
        stats.add(base + 2.0);
        assertThat(stats.mean(), closeTo(base + 1.0, 1e-3));
        assertThat(stats.sampleVariance(), closeTo(1.0, 1e-3));
    }

    public void testMergeIntoEmptyAccumulator() {
        WelfordVariance populated = new WelfordVariance();
        populated.add(10.0);
        populated.add(20.0);
        populated.add(30.0);

        WelfordVariance empty = new WelfordVariance();
        empty.merge(populated);
        assertThat(empty.mean(), closeTo(populated.mean(), 1e-10));
        assertThat(empty.sampleVariance(), closeTo(populated.sampleVariance(), 1e-10));
        assertEquals(populated.count(), empty.count());
    }

    public void testMergeBothEmptyIsNoOp() {
        WelfordVariance left = new WelfordVariance();
        WelfordVariance right = new WelfordVariance();
        left.merge(right);
        assertEquals(0, left.count());
        assertThat(left.mean(), closeTo(0.0, 1e-10));
        assertThat(left.sampleVariance(), closeTo(0.0, 1e-10));
    }

    public void testMergeOrderIsSymmetric() {
        WelfordVariance left = new WelfordVariance();
        left.add(1.0);
        left.add(3.0);
        left.add(5.0);
        WelfordVariance right = new WelfordVariance();
        right.add(7.0);
        right.add(9.0);

        WelfordVariance leftFirst = new WelfordVariance();
        leftFirst.merge(left);
        leftFirst.merge(right);

        WelfordVariance rightFirst = new WelfordVariance();
        rightFirst.merge(right);
        rightFirst.merge(left);

        assertThat(leftFirst.mean(), closeTo(rightFirst.mean(), 1e-10));
        assertThat(leftFirst.sampleVariance(), closeTo(rightFirst.sampleVariance(), 1e-10));
        assertEquals(leftFirst.count(), rightFirst.count());
    }

    public void testAdvanceToObservationOnlyAdvancesCount() {
        WelfordVariance stats = new WelfordVariance();
        stats.add(2.0);
        stats.add(4.0);
        double meanBefore = stats.mean();
        double m2Before = stats.m2();

        stats.advanceToObservation(5);
        assertEquals(5, stats.count());
        assertThat(stats.mean(), closeTo(meanBefore, 1e-10));
        assertThat(stats.m2(), closeTo(m2Before, 1e-10));
    }

    public void testAddAsObservationWithNonPositiveIndexIsIgnored() {
        WelfordVariance stats = new WelfordVariance();
        stats.addAsObservation(99.0, 0);
        stats.addAsObservation(99.0, -1);
        assertEquals(0, stats.count());
        assertThat(stats.mean(), closeTo(0.0, 1e-10));
    }

    public void testAddAsObservationMatchesSequentialAddWithoutSkips() {
        double[] values = { 2.0, 5.0, 11.0, 13.0 };
        WelfordVariance sequential = new WelfordVariance();
        for (double value : values) {
            sequential.add(value);
        }

        WelfordVariance indexed = new WelfordVariance();
        for (int i = 0; i < values.length; i++) {
            indexed.addAsObservation(values[i], i + 1L);
        }

        assertThat(indexed.mean(), closeTo(sequential.mean(), 1e-10));
        assertThat(indexed.m2(), closeTo(sequential.m2(), 1e-10));
        assertEquals(sequential.count(), indexed.count());
    }

    public void testAddAsObservationAfterAdvanceMatchesClusterSizePattern() {
        // Mirrors DiskBBQ writers: empty clusters advance the counter; non-empty clusters record size.
        // Mean is taken over all observation slots (including empty clusters), not only non-empty sizes.
        int[] clusterSizes = { 0, 10, 0, 20, 30, 0, 40 };
        WelfordVariance stats = new WelfordVariance();
        int observationNumber = 0;
        for (int size : clusterSizes) {
            observationNumber += 1;
            if (size == 0) {
                stats.advanceToObservation(observationNumber);
            } else {
                stats.addAsObservation(size, observationNumber);
            }
        }

        assertEquals(clusterSizes.length, stats.count());
        assertThat(stats.mean(), closeTo(16.857142857142858, 1e-10));
        assertThat(stats.sampleVariance(), closeTo(1204.857142857142857 / 6.0, 1e-6));
    }
}
