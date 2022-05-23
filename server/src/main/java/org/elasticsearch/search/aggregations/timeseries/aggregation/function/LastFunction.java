/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesLast;

import java.util.Map;

public class LastFunction implements AggregatorFunction<TimePoint, Double> {
    private TimePoint point;

    @Override
    public void collect(TimePoint value) {
        if (this.point == null || value.getTimestamp() > this.point.getTimestamp()) {
            this.point = value;
        }
    }

    @Override
    public Double get() {
        return point.getValue();
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesLast(TimeSeriesLast.NAME, point.getValue(), point.getTimestamp(), formatter, metadata);
    }
}
