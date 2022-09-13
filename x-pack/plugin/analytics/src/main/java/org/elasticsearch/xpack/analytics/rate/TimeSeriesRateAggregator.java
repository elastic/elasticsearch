/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
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
import java.util.Objects;

public class TimeSeriesRateAggregator extends NumericMetricsAggregator.SingleValue {

    protected final ValuesSource.Numeric valuesSource;
    protected DoubleArray values;

    long currentBucket = -1;
    long currentEndTime = -1;
    long currentStartTime = -1;
    double resetCompensation = 0;
    double currentEndValue = -1;
    double currentStartValue = -1;
    BytesRef currentTsid = null;        // TODO use global ordinals for faster tsid comparison

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
        this.values = bigArrays().newDoubleArray(1, true);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRate(name, 0.0, 1.0, DocValueFormat.RAW, metadata());
    }

    private void calculateLastBucket() {
        if (currentBucket != -1) {
            System.out.println("currentEnd " + currentEndValue + " currentStart " + currentStartValue + " comp " + resetCompensation);
            long timespan = currentEndTime - currentStartTime;
            double increase = currentEndValue - currentStartValue + resetCompensation;
            double rate = timespan == 0 ? Double.NaN : increase / timespan;
            values.set(currentBucket, rate);
            System.out.println("bucket" + currentBucket + " increase " + increase + " timespan " + timespan + " rate " + rate);
        }
    }

    private double checkForResets(double latestValue) {
        if (latestValue > currentStartValue) {
            // reset detected
            resetCompensation += currentEndValue;
            currentEndValue = latestValue;
            System.out.println("reset! currentEnd " + currentEndValue + " currentStart " + currentStartValue + " comp " + resetCompensation);
        }
        return latestValue;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        SortedNumericDoubleValues leafValues = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                leafValues.advanceExact(doc);   // TODO handle missing values
                double latestValue = leafValues.nextValue();   // assume singleton values

                if (bucket != currentBucket) {
                    values = bigArrays().grow(values, bucket + 1);
                    if (Objects.equals(currentTsid, aggCtx.getTsid()) == false) {
                        // if we're on a new tsid then we need to calculate the last bucket
                        System.out.println("new tsid " + aggCtx.getTsid().utf8ToString());
                        calculateLastBucket();
                        currentTsid = aggCtx.getTsid();
                    } else {
                        // if we're in a new bucket but in the same tsid then we update the
                        // timestamp and last value before we calculate the last bucket
                        System.out.println("new bucket");
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
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        calculateLastBucket();
        return new InternalRate(name, values.get(owningBucketOrd), 1, DocValueFormat.RAW, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(values);
    }

    @Override
    public double metric(long owningBucketOrd) {
        return values.get(owningBucketOrd);
    }
}
