/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeSeriesCountValues extends InternalAggregation {
    public static final String NAME = "time_series_count_values";

    private final Map<Long, AtomicInteger> valueCount;
    private final DocValueFormat formatter;

    public TimeSeriesCountValues(String name, Map<Long, AtomicInteger> valueCount, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.valueCount = valueCount;
        this.formatter = formatter;
    }

    public TimeSeriesCountValues(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        valueCount = in.readOrderedMap(input -> input.readLong(), input -> new AtomicInteger(input.readInt()));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Map<Long, AtomicInteger> getValueCount() {
        return valueCount;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        out.writeMap(valueCount, (out1, value) -> out1.writeLong(value), (out1, value) -> out1.writeInt(value.get()));
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {

        Map<Long, AtomicInteger> reduced = new HashMap<>();
        for (InternalAggregation internalAggregation : aggregations) {
            TimeSeriesCountValues timeSeriesCountValues = (TimeSeriesCountValues) internalAggregation;
            if (reduced.isEmpty()) {
                reduced.putAll(timeSeriesCountValues.valueCount);
            } else {
                timeSeriesCountValues.valueCount.forEach((value, count) -> {
                    if (reduced.containsKey(value)) {
                        AtomicInteger current = reduced.get(value);
                        current.addAndGet(count.get());
                    } else {
                        reduced.put(value, count);
                    }
                });
            }
        }
        return new TimeSeriesCountValues(name, reduced, formatter, getMetadata());
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<Long, AtomicInteger> value : valueCount.entrySet()) {
            builder.field(String.valueOf(Double.longBitsToDouble(value.getKey())), value.getValue().get());
        }
        return builder;
    }
}
