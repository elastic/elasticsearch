/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;

import java.io.PrintStream;

public final class EmptyDoubleHdrHistogram extends DoubleHistogram {

    public EmptyDoubleHdrHistogram() {
        super(0);
        setAutoResize(false);
    }

    @Override
    public void setAutoResize(boolean ignored) {
        // DO NOT CHANGE 'autoResize'
    }

    @Override
    public void recordValue(double value) throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void recordValueWithCount(double value, long count) throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void recordValueWithExpectedInterval(double value, double expectedIntervalBetweenValueSamples)
        throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public DoubleHistogram copy() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public DoubleHistogram copyCorrectedForCoordinatedOmission(double expectedIntervalBetweenValueSamples) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void copyInto(DoubleHistogram targetHistogram) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void copyIntoCorrectedForCoordinatedOmission(DoubleHistogram targetHistogram, double expectedIntervalBetweenValueSamples) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void add(DoubleHistogram fromHistogram) throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void addWhileCorrectingForCoordinatedOmission(DoubleHistogram fromHistogram, double expectedIntervalBetweenValueSamples) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void subtract(DoubleHistogram otherHistogram) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double lowestEquivalentValue(double value) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double highestEquivalentValue(double value) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double sizeOfEquivalentValueRange(double value) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double medianEquivalentValue(double value) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double nextNonEquivalentValue(double value) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public boolean valuesAreEquivalent(double value1, double value2) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void setStartTimeStamp(long timeStampMsec) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public long getStartTimeStamp() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public long getEndTimeStamp() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void setEndTimeStamp(long timeStampMsec) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void setTag(String tag) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getMinValue() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getMaxValue() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getMinNonZeroValue() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getMaxValueAsDouble() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getMean() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public double getStdDeviation() {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public Percentiles percentiles(int percentileTicksPerHalfDistance) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public LinearBucketValues linearBucketValues(double valueUnitsPerBucket) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public LogarithmicBucketValues logarithmicBucketValues(double valueUnitsInFirstBucket, double logBase) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void outputPercentileDistribution(PrintStream printStream, Double outputValueUnitScalingRatio) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void outputPercentileDistribution(
        PrintStream printStream,
        int percentileTicksPerHalfDistance,
        Double outputValueUnitScalingRatio
    ) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

    @Override
    public void outputPercentileDistribution(
        PrintStream printStream,
        int percentileTicksPerHalfDistance,
        Double outputValueUnitScalingRatio,
        boolean useCsvFormat
    ) {
        throw new UnsupportedOperationException("Immutable Empty HdrHistogram");
    }

}
