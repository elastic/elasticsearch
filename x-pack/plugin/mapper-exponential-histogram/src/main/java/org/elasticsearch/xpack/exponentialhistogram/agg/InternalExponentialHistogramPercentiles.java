/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalExponentialHistogramPercentiles extends InternalNumericMetricsAggregation.MultiValue {
    public static final String NAME = "exponential_histogram_percentiles";

    private final double[] percentiles;
    final InternalExponentialHistogram histogram;
    private final boolean keyed;

    public InternalExponentialHistogramPercentiles(
        InternalExponentialHistogram histogram,
        boolean keyed,
        double[] percentiles
    ) {
        super(histogram.getName(), histogram.format, histogram.getMetadata());
        this.percentiles = percentiles;
        this.histogram = histogram;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    public InternalExponentialHistogramPercentiles(StreamInput in) throws IOException {
        super(in, true);
        percentiles = in.readDoubleArray();
        keyed = in.readBoolean();
        histogram = new InternalExponentialHistogram(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(percentiles);
        out.writeBoolean(keyed);
        histogram.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static InternalExponentialHistogramPercentiles empty(
        String name,
        double[] keys,
        boolean keyed,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        final InternalExponentialHistogram histogram = new InternalExponentialHistogram(name, 1, 1, format, metadata);
        return new InternalExponentialHistogramPercentiles(histogram, keyed, keys);
    }

    @Override
    public InternalExponentialHistogramPercentiles reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        InternalExponentialHistogram histogram = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalExponentialHistogramPercentiles percentiles = (InternalExponentialHistogramPercentiles)aggregation;
            if (histogram == null) {
                histogram = new InternalExponentialHistogram(
                    percentiles.getName(),
                    percentiles.histogram.maxBuckets,
                    percentiles.histogram.currentScale,
                    percentiles.format,
                    percentiles.metadata
                );
            }
            for (InternalExponentialHistogram.Bucket bucket : percentiles.histogram.getBuckets()) {
                double value = bucket.getUpperBound();
                if (value < 0) {
                    value = bucket.getLowerBound();
                }
                histogram.add(value, bucket.getCount());
            }
        }
        return new InternalExponentialHistogramPercentiles(histogram, keyed, percentiles);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for (double v : percentiles) {
                String key = String.valueOf(v);
                double value = value(v);
                builder.field(key, value);

                //builder.field(key, state.getTotalCount() == 0 ? null : value);
                //if (format != DocValueFormat.RAW && state.getTotalCount() > 0) {
                //    builder.field(key + "_as_string", format.format(value).toString());
                //}
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (double key : percentiles) {
                double value = value(key);
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
                builder.field(CommonFields.VALUE.getPreferredName(), value);
                //builder.field(CommonFields.VALUE.getPreferredName(), state.getTotalCount() == 0 ? null : value);
                //if (format != DocValueFormat.RAW && state.getTotalCount() > 0) {
                //    builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
                //}
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public Iterable<String> valueNames() {
        return List.of(percentiles).stream().map(Object::toString).toList();
    }

    @Override
    public double value(String key) {
        return value(Double.parseDouble(key));
    }

    public double value(double key) {
        final long totalCount = histogram.getTotalCount();
        final long threshold = (long)(totalCount * (key / 100.0));
        long cumulativeCount = 0;
        for (InternalExponentialHistogram.Bucket bucket : histogram.getBuckets()) {
            cumulativeCount += bucket.getCount();
            if (cumulativeCount >= threshold) {
                if (bucket.negative) {
                    return bucket.getLowerBound();
                }
                return bucket.getUpperBound();
            }
        }
        return 0;
    }
}
