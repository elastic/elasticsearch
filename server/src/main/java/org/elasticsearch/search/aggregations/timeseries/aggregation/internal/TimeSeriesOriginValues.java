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
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesOriginValues extends InternalAggregation implements Comparable<TimeSeriesOriginValues> {
    public static final String NAME = "time_series_origin_values";

    private final List<TimePoint> timePoints;
    private final DocValueFormat formatter;

    public TimeSeriesOriginValues(String name, List<TimePoint> timePoints, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.timePoints = timePoints;
        this.formatter = formatter;
    }

    public TimeSeriesOriginValues(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        timePoints = in.readList(TimePoint::new);
    }

    public List<TimePoint> getTimePoints() {
        return timePoints;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        out.writeList(timePoints);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        List<TimePoint> allTimePoints = new ArrayList<>();

        List<TimeSeriesOriginValues> timeSeriesOriginValues = aggregations.stream()
            .map(c -> (TimeSeriesOriginValues) c)
            .sorted()
            .collect(Collectors.toList());
        timeSeriesOriginValues.stream().forEach(v -> allTimePoints.addAll(v.timePoints));
        return new TimeSeriesOriginValues(name, allTimePoints, formatter, getMetadata());
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
        builder.field(CommonFields.VALUE.getPreferredName());
        builder.startObject();
        for (TimePoint timePoint : timePoints) {
            builder.field(String.valueOf(timePoint.getTimestamp()), timePoint.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int compareTo(TimeSeriesOriginValues o) {
        if (timePoints.size() == 0) {
            return -1;
        } else if (o.timePoints.size() == 0) {
            return 1;
        }
        return (int) (timePoints.get(0).getTimestamp() - o.timePoints.get(0).getTimestamp());
    }
}
