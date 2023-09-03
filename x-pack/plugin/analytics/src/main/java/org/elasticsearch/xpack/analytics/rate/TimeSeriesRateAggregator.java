/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class TimeSeriesRateAggregator extends NumericMetricsAggregator.SingleValue {

    protected final ValuesSource.Numeric valuesSource;

    protected DoubleArray startValues;
    protected DoubleArray endValues;
    protected LongArray startTimes;
    protected LongArray endTimes;
    protected DoubleArray resetCompensations;

    private long currentBucket = -1;
    private long currentEndTime = -1;
    private long currentStartTime = -1;
    private double resetCompensation = 0;
    private double currentEndValue = -1;
    private double currentStartValue = -1;
    private int currentTsid = -1;

    private final Rounding.DateTimeUnit rateUnit;

    // Unused parameters are so that the constructor implements `RateAggregatorSupplier`
    protected TimeSeriesRateAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        Rounding.DateTimeUnit rateUnit,
        RateMode rateMode,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        this.startValues = bigArrays().newDoubleArray(1, true);
        this.endValues = bigArrays().newDoubleArray(1, true);
        this.startTimes = bigArrays().newLongArray(1, true);
        this.endTimes = bigArrays().newLongArray(1, true);
        this.resetCompensations = bigArrays().newDoubleArray(1, true);
        this.rateUnit = rateUnit;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalResetTrackingRate(name, DocValueFormat.RAW, metadata(), 0, 0, 0, 0, 0, Rounding.DateTimeUnit.SECOND_OF_MINUTE);
    }

    private void calculateLastBucket() {
        if (currentBucket != -1) {
            startValues.set(currentBucket, currentStartValue);
            endValues.set(currentBucket, currentEndValue);
            startTimes.set(currentBucket, currentStartTime);
            endTimes.set(currentBucket, currentEndTime);
            resetCompensations.set(currentBucket, resetCompensation);
            currentBucket = -1;
        }
    }

    private double checkForResets(double latestValue) {
        if (latestValue > currentStartValue) {
            // reset detected
            resetCompensation += currentEndValue;
            currentEndValue = latestValue;
        }
        return latestValue;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        NumericDoubleValues leafValues = MultiValueMode.MAX.select(valuesSource.doubleValues(aggCtx.getLeafReaderContext()));
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                leafValues.advanceExact(doc);   // TODO handle missing values
                double latestValue = leafValues.doubleValue();

                if (bucket != currentBucket) {
                    startValues = bigArrays().grow(startValues, bucket + 1);
                    endValues = bigArrays().grow(endValues, bucket + 1);
                    startTimes = bigArrays().grow(startTimes, bucket + 1);
                    endTimes = bigArrays().grow(endTimes, bucket + 1);
                    resetCompensations = bigArrays().grow(resetCompensations, bucket + 1);
                    if (currentTsid != aggCtx.getTsidOrd()) {
                        // if we're on a new tsid then we need to calculate the last bucket
                        calculateLastBucket();
                        currentTsid = aggCtx.getTsidOrd();
                    } else {
                        // if we're in a new bucket but in the same tsid then we update the
                        // timestamp and last value before we calculate the last bucket
                        currentStartTime = aggCtx.getTimestamp();
                        currentStartValue = checkForResets(latestValue);
                        calculateLastBucket();
                    }
                    currentBucket = bucket;
                    currentStartTime = currentEndTime = aggCtx.getTimestamp();
                    currentStartValue = currentEndValue = latestValue;
                    resetCompensation = 0;
                } else {
                    currentStartTime = aggCtx.getTimestamp();
                    currentStartValue = checkForResets(latestValue);
                }
            }
        };
    }

    @Override
    public InternalResetTrackingRate buildAggregation(long owningBucketOrd) {
        calculateLastBucket();
        return new InternalResetTrackingRate(
            name,
            DocValueFormat.RAW,
            metadata(),
            startValues.get(owningBucketOrd),
            endValues.get(owningBucketOrd),
            startTimes.get(owningBucketOrd),
            endTimes.get(owningBucketOrd),
            resetCompensations.get(owningBucketOrd),
            rateUnit == null ? Rounding.DateTimeUnit.SECOND_OF_MINUTE : rateUnit
        );
    }

    @Override
    protected void doClose() {
        Releasables.close(startValues, endValues, startTimes, endTimes, resetCompensations);
    }

    @Override
    public double metric(long owningBucketOrd) {
        return buildAggregation(owningBucketOrd).getValue();
    }
}
