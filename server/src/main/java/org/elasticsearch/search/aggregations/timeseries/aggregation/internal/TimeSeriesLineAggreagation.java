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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TimeSeriesLineAggreagation extends InternalAggregation {
    public static final String NAME = "time_series_line";

    private final Map<Long, InternalAggregation> timeBucketValues;
    private final DocValueFormat formatter;

    public TimeSeriesLineAggreagation(
        String name,
        Map<Long, InternalAggregation> timeBucketValues,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.timeBucketValues = timeBucketValues;
        this.formatter = formatter;
    }

    public TimeSeriesLineAggreagation(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        timeBucketValues = in.readOrderedMap(StreamInput::readLong, stream -> stream.readNamedWriteable(InternalAggregation.class));
    }

    public Map<Long, InternalAggregation> getTimeBucketValues() {
        return timeBucketValues;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        out.writeMap(timeBucketValues, StreamOutput::writeLong, StreamOutput::writeNamedWriteable);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        Map<Long, List<InternalAggregation>> timeBucketAggregationsList = new TreeMap<>();
        Map<Long, InternalAggregation> timeBucketResults = new TreeMap<>();
        for (InternalAggregation internalAggregation : aggregations) {
            TimeSeriesLineAggreagation timeSeriesLineAggreagation = (TimeSeriesLineAggreagation) internalAggregation;
            for (Entry<Long, InternalAggregation> entry : timeSeriesLineAggreagation.timeBucketValues.entrySet()) {
                Long timestamp = entry.getKey();
                InternalAggregation value = entry.getValue();
                List<InternalAggregation> values = timeBucketAggregationsList.get(timestamp);
                if (values == null) {
                    values = new ArrayList<>();
                    timeBucketAggregationsList.put(timestamp, values);
                }
                values.add(value);
            }
        }

        timeBucketAggregationsList.forEach((timestamp, aggs) -> {
            if (aggs.size() > 0) {
                InternalAggregation first = aggs.get(0);
                timeBucketResults.put(timestamp, first.reduce(aggs, reduceContext));
            }
        });

        return new TimeSeriesLineAggreagation(name, timeBucketResults, formatter, getMetadata());
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
        for (Entry<Long, InternalAggregation> entry : timeBucketValues.entrySet()) {
            builder.startObject(String.valueOf(entry.getKey()));
            entry.getValue().doXContentBody(builder, params);
            builder.endObject();
        }
        return builder;
    }
}
