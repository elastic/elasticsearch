/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregation.Avg;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregation.Function;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregation.ValueCount;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregationAggregator;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AggregateMetricTimeSeriesAggregationAggregator extends TimeSeriesAggregationAggregator {
    private final AggregateMetricsValuesSource.AggregateDoubleMetric valuesSource;

    public AggregateMetricTimeSeriesAggregationAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        List<String> group,
        List<String> without,
        DateHistogramInterval interval,
        DateHistogramInterval offset,
        String aggregator,
        DateHistogramInterval downsampleRange,
        String downsampleFunction,
        BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            keyed,
            group,
            without,
            interval,
            offset,
            aggregator,
            downsampleRange,
            downsampleFunction,
            bucketCountThresholds,
            order,
            null,
            context,
            parent,
            bucketCardinality,
            metadata
        );
        this.valuesSource = valuesSourceConfig.hasValues()
            ? (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSourceConfig.getValuesSource()
            : null;
        this.format = valuesSourceConfig.format();
        if (this.downsampleFunction == null) {
            this.downsampleFunction = Function.avg;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub, AggregationExecutionContext aggCtx)
        throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        Metric metricType = getAggregateMetric();
        if (metricType != null) {
            final SortedNumericDoubleValues values = valuesSource.getAggregateMetricValues(context, metricType);
            return new Collector(sub, values, aggCtx, (doc) -> {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        AggregatorFunction function = getAggregatorFunction();
                        function.collect(value);
                    }
                }
            });
        } else if (metricType == Metric.value_count) {
            final SortedNumericDoubleValues values = valuesSource.getAggregateMetricValues(context, metricType);
            return new Collector(sub, values, aggCtx, (doc) -> {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        AggregatorFunction function = getAggregatorFunction();
                        ((ValueCountFunction) function).collectExact((long) value);
                    }
                }
            });
        } else {
            final SortedNumericDoubleValues aggregateSums = valuesSource.getAggregateMetricValues(context, Metric.sum);
            final SortedNumericDoubleValues aggregateValueCounts = valuesSource.getAggregateMetricValues(context, Metric.value_count);
            return new Collector(sub, aggregateSums, aggCtx, (doc) -> {
                double sum = 0;
                long valueCount = 0;
                if (aggregateSums.advanceExact(doc)) {
                    final int valuesCount = aggregateSums.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = aggregateSums.nextValue();
                        sum += value;
                    }
                }

                if (aggregateValueCounts.advanceExact(doc)) {
                    final int valuesCount = aggregateValueCounts.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = aggregateValueCounts.nextValue();
                        valueCount += value;
                    }
                }

                AggregatorFunction function = getAggregatorFunction();
                ((AvgFunction) function).collectExact(sum, valueCount);
            });
        }
    }

    private Metric getAggregateMetric() {
        switch (downsampleFunction) {
            case max:
                return Metric.max;
            case min:
                return Metric.min;
            case sum:
                return Metric.sum;
            case count:
                return Metric.value_count;
        }
        return null;
    }
}
