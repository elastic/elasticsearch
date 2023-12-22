/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public final class ExponentialHistogramPercentilesAggregator extends NumericMetricsAggregator.MultiValue {

    private final ExponentialHistogramValuesSource.Histogram valuesSource;
    private final DocValueFormat formatter;

    private final int maxBuckets;
    private final int maxScale;
    private final double[] percentiles;
    private final ArrayList<InternalExponentialHistogramPercentiles> aggregations;

    public ExponentialHistogramPercentilesAggregator(
        String name,
        AggregatorFactories factories,
        int maxBuckets,
        int maxScale,
        double[] percentiles,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = (ExponentialHistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource();
        this.formatter = valuesSourceConfig.format();
        this.maxBuckets = maxBuckets;
        this.maxScale = maxScale;
        this.percentiles = percentiles;
        this.aggregations = new ArrayList<>(1);

        // Sub aggregations are not allowed when running histogram agg over histograms
        if (subAggregators().length > 0) {
            throw new IllegalArgumentException("Histogram aggregation on histogram fields does not support sub-aggregations");
        }
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesConfig.indexOfKey(percentiles, Double.parseDouble(name)) >= 0;
    }

    @Override
    public double metric(String name, long bucketOrd) {
        final InternalExponentialHistogramPercentiles percentiles = aggregations.get((int)bucketOrd);
        if (percentiles == null) {
            return Double.NaN;
        } else {
            return percentiles.value(name);
        }
    }

    @Override
    public InternalExponentialHistogramPercentiles buildAggregation(long owningBucketOrd) throws IOException {
        if (owningBucketOrd >= aggregations.size()) {
            return buildEmptyAggregation();
        }
        return aggregations.get((int)owningBucketOrd);
    }

    @Override
    public InternalExponentialHistogramPercentiles buildEmptyAggregation() {
        return new InternalExponentialHistogramPercentiles(
            new InternalExponentialHistogram(name(), maxBuckets, maxScale, formatter, metadata()),
            true,
            percentiles
        );
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                while (owningBucketOrd >= aggregations.size()) {
                    aggregations.add(buildEmptyAggregation());
                }
                final InternalExponentialHistogramPercentiles aggregation = aggregations.get((int)owningBucketOrd);
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    while (sketch.next()) {
                        final double value = sketch.value();
                        final long count = sketch.count();
                        aggregation.histogram.add(sketch.value(), sketch.count());
                    }
                }
            }
        };
    }
}
