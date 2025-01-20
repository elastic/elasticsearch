/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;

/**
 * Detects spikes and dips in a time series.
 */
final class SpikeAndDipDetector {

    private record SpikeOrDip(int index, int startExcluded, int endExcluded) {
        double value(double[] values) {
            return values[index];
        }

        boolean includes(int i) {
            return i >= startExcluded && i < endExcluded;
        }
    }

    private int argmax(double[] values, int start, int end, boolean negate) {
        int argmax = 0;
        double max = negate ? -values[0] : values[0];
        for (int i = 1; i < values.length; i++) {
            double value = negate ? -values[i] : values[i];
            if (value > max) {
                argmax = i;
                max = value;
            }
        }
        return argmax;
    }

    private double sum(double[] values, int start, int end, boolean negate) {
        double sum = 0.0;
        for (int i = start; i < end; i++) {
            sum += values[i];
        }
        return negate ? -sum : sum;
    }

    private SpikeOrDip findSpikeOrDip(double[] values, int extent, boolean negate) {

        extent = Math.min(extent, values.length - 1);

        final int argmax = argmax(values, 0, values.length, negate);

        // Find the maximum average interval of width extent which includes argmax.
        int maxStart = Math.max(0, argmax + 1 - extent);
        int maxEnd = Math.min(maxStart + extent, values.length);
        double maxSum = sum(values, maxStart, maxEnd, negate);
        for (int start = maxStart + 1; start <= argmax; start++) {
            if (start + extent > values.length) {
                break;
            }
            double average = sum(values, start, start + extent, negate);
            if (average > maxSum) {
                maxStart = start;
                maxSum = average;
            }
        }

        return new SpikeOrDip(argmax, maxStart, maxStart + extent);
    }

    private interface ExcludedPredicate {
        boolean exclude(int index);
    }

    private double[] removeIf(ExcludedPredicate should, double[] values) {
        int numKept = 0;
        for (int i = 0; i < values.length; i++) {
            if (should.exclude(i) == false) {
                numKept++;
            }
        }
        double[] newValues = new double[numKept];
        int j = 0;
        for (int i = 0; i < values.length; i++) {
            if (should.exclude(i) == false) {
                newValues[j++] = values[i];
            }
        }
        return newValues;
    }

    private final MlAggsHelper.DoubleBucketValues bucketValues;
    private final int numValues;
    private final int dipIndex;
    private final int spikeIndex;
    private final double dipValue;
    private final double spikeValue;
    private final KDE spikeTestKDE;
    private final KDE dipTestKDE;

    SpikeAndDipDetector(MlAggsHelper.DoubleBucketValues bucketValues) {
        this.bucketValues = bucketValues;
        double[] values = bucketValues.getValues();

        numValues = values.length;

        if (values.length < 4) {
            dipIndex = -1;
            spikeIndex = -1;
            dipValue = Double.NaN;
            spikeValue = Double.NaN;
            spikeTestKDE = null;
            dipTestKDE = null;
            return;
        }

        // Usually roughly 10% of values.
        int spikeLength = Math.max((int) (0.1 * values.length + 0.5), 2) - 1;

        SpikeOrDip dip = findSpikeOrDip(values, spikeLength, true);
        SpikeOrDip spike = findSpikeOrDip(values, spikeLength, false);

        dipIndex = dip.index();
        spikeIndex = spike.index();
        dipValue = dip.value(values);
        spikeValue = spike.value(values);

        double[] dipKDEValues = removeIf((i) -> (dip.includes(i) || i == spike.index()), values);
        double[] spikeKDEValues = removeIf((i) -> (spike.includes(i) || i == dip.index()), values);
        Arrays.sort(dipKDEValues);
        Arrays.sort(spikeKDEValues);

        // We purposely over smooth to only surface visually significant spikes and dips.
        dipTestKDE = new KDE(dipKDEValues, 1.36);
        spikeTestKDE = new KDE(spikeKDEValues, 1.36);
    }

    ChangeType detect(double pValueThreshold) {
        if (dipIndex == -1 || spikeIndex == -1) {
            return new ChangeType.Indeterminable(
                "not enough buckets to check for dip or spike. Requires at least [3]; found [" + numValues + "]"
            );
        }

        KDE.ValueAndMagnitude dipLeftTailTest = dipTestKDE.cdf(dipValue);
        KDE.ValueAndMagnitude spikeRightTailTest = spikeTestKDE.sf(spikeValue);
        double dipPValue = dipLeftTailTest.pValue(numValues);
        double spikePValue = spikeRightTailTest.pValue(numValues);

        if (dipPValue < pValueThreshold && spikePValue < pValueThreshold) {
            if (dipLeftTailTest.isMoreSignificant(spikeRightTailTest)) {
                return new ChangeType.Dip(dipPValue, bucketValues.getBucketIndex(dipIndex));
            }
            return new ChangeType.Spike(spikePValue, bucketValues.getBucketIndex(spikeIndex));
        }
        if (dipPValue < pValueThreshold) {
            return new ChangeType.Dip(dipPValue, bucketValues.getBucketIndex(dipIndex));
        }
        if (spikePValue < pValueThreshold) {
            return new ChangeType.Spike(spikePValue, bucketValues.getBucketIndex(spikeIndex));
        }
        return new ChangeType.Stationary();
    }

    double dipValue() {
        return dipValue;
    }

    double spikeValue() {
        return spikeValue;
    }

    KDE spikeTestKDE() {
        return spikeTestKDE;
    }

    KDE dipTestKDE() {
        return dipTestKDE;
    }
}
