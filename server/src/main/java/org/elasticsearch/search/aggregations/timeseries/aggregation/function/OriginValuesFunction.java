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
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesOriginValues;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OriginValuesFunction implements AggregatorFunction<TimePoint, List<TimePoint>> {
    private List<TimePoint> timePoints;

    public OriginValuesFunction() {
        this.timePoints = new ArrayList<>();
    }

    @Override
    public void collect(TimePoint value) {
     this.timePoints.add(value);
    }

    @Override
    public List<TimePoint> get() {
        return timePoints;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesOriginValues(TimeSeriesOriginValues.NAME, timePoints, formatter, metadata);
    }
}
