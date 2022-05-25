/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
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
import org.elasticsearch.search.aggregations.timeseries.aggregation.Downsample;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Function;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimeSeriesAggregationAggregator;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings({ "unchecked", "rawtypes" })
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
        org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator aggregator,
        Map<String, Object> aggregatorParams,
        Downsample downsample,
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
            aggregatorParams,
            downsample,
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
            this.downsampleFunction = Function.avg_over_time;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub, AggregationExecutionContext aggCtx)
        throws IOException {
        if (valuesSource == null) {
            return new LeafBucketCollector() {
                @Override
                public void setScorer(Scorable arg0) throws IOException {
                    // no-op
                }

                @Override
                public void collect(int doc, long bucket) {
                    // no-op
                }

                @Override
                public boolean isNoop() {
                    return false;
                }
            };
        }
        Metric metricType = getAggregateMetric();
        if (metricType != null) {
            final SortedNumericDoubleValues values = valuesSource.getAggregateMetricValues(context, metricType);
            return new Collector(sub, values, aggCtx, (doc) -> {
                if (aggCtx.getTimestamp() + downsampleRange < preRounding) {
                    return;
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        if (false == timeBucketMetrics.containsKey(preRounding)) {
                            downsampleParams.put(Function.ROUNDING_FIELD, preRounding);
                            timeBucketMetrics.put(preRounding, downsampleFunction.getFunction(downsampleParams));
                        }
                        for (Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
                            Long timestamp = entry.getKey();
                            AggregatorFunction function = entry.getValue();
                            if (aggCtx.getTimestamp() + downsampleRange >= timestamp) {
                                if (function instanceof ValueCountFunction) {
                                    ((ValueCountFunction) function).collectExact((long) value);
                                } else {
                                    function.collect(value);
                                }
                            } else {
                                break;
                            }
                        }
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

                if (false == timeBucketMetrics.containsKey(preRounding)) {
                    downsampleParams.put(Function.ROUNDING_FIELD, preRounding);
                    timeBucketMetrics.put(preRounding, downsampleFunction.getFunction(downsampleParams));
                }
                for (Entry<Long, AggregatorFunction> entry : timeBucketMetrics.entrySet()) {
                    Long timestamp = entry.getKey();
                    AggregatorFunction function = entry.getValue();
                    if (aggCtx.getTimestamp() + downsampleRange >= timestamp) {
                        ((AvgFunction) function).collectExact(sum, valueCount);
                    } else {
                        break;
                    }
                }
            });
        }
    }

    private Metric getAggregateMetric() {
        switch (downsampleFunction) {
            case max_over_time:
                return Metric.max;
            case min_over_time:
                return Metric.min;
            case sum_over_time:
                return Metric.sum;
            case count_over_time:
                return Metric.value_count;
        }
        return null;
    }
}
