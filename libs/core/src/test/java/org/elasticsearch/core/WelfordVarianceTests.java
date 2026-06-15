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
}
