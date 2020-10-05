/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.histogram.SizedBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class RateAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;
    private final Rounding.DateTimeUnit rateUnit;
    private final SizedBucketAggregator sizedBucketAggregator;

    private DoubleArray sums;
    private DoubleArray compensations;

    public RateAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        Rounding.DateTimeUnit rateUnit,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
            compensations = context.bigArrays().newDoubleArray(1, true);
        }
        this.rateUnit = rateUnit;
        this.sizedBucketAggregator = findSizedBucketAncestor();
    }

    private SizedBucketAggregator findSizedBucketAncestor() {
        SizedBucketAggregator sizedBucketAggregator = null;
        for (Aggregator ancestor = parent; ancestor != null; ancestor = ancestor.parent()) {
            if (ancestor instanceof SizedBucketAggregator) {
                sizedBucketAggregator = (SizedBucketAggregator) ancestor;
                break;
            }
        }
        if (sizedBucketAggregator == null) {
            throw new IllegalArgumentException("The rate aggregation can only be used inside a date histogram");
        }
        return sizedBucketAggregator;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (sizedBucketAggregator == null || valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd) / sizedBucketAggregator.bucketSize(owningBucketOrd, rateUnit);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalRate(name, sums.get(bucket), sizedBucketAggregator.bucketSize(bucket, rateUnit), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRate(name, 0.0, 1.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
}
