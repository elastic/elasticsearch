/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.Metric;

import java.io.IOException;
import java.util.Map;

class AggregateMetricBackedMinAggregator extends NumericMetricsAggregator.SingleValue {

    private final AggregateMetricsValuesSource.AggregateMetricDouble valuesSource;
    final DocValueFormat format;
    DoubleArray mins;

    AggregateMetricBackedMinAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = (AggregateMetricsValuesSource.AggregateMetricDouble) config.getValuesSource();
        mins = context.bigArrays().newDoubleArray(1, false);
        mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        this.format = config.format();
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final BigArrays bigArrays = bigArrays();
        final SortedNumericDoubleValues allValues = valuesSource.getAggregateMetricValues(aggCtx.getLeafReaderContext(), Metric.min);
        final NumericDoubleValues values = MultiValueMode.MIN.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= mins.size()) {
                    long from = mins.size();
                    mins = bigArrays.grow(mins, bucket + 1);
                    mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
                }
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double min = mins.get(bucket);
                    min = Math.min(min, value);
                    mins.set(bucket, min);
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (owningBucketOrd >= mins.size()) {
            return Double.POSITIVE_INFINITY;
        }
        return mins.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= mins.size()) {
            return buildEmptyAggregation();
        }
        return new Min(name, mins.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return Min.createEmptyMin(name, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(mins);
    }
}
