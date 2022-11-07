/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import java.util.Map;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesLast;

public abstract class AbstractLastFunction implements AggregatorFunction<TimePoint, Double> {
    protected TimePoint point;

    @Override
    public void collect(TimePoint value) {
        if (this.point == null || value.getTimestamp() > this.point.getTimestamp()) {
            this.point = value;
        }
    }

    @Override
    public Double get() {
        return interGet();
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesLast(TimeSeriesLast.NAME, interGet(), point.getTimestamp(), formatter, metadata);
    }

    protected abstract Double interGet();

    protected TimePoint getPoint() {
        return point;
    }
}

