/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;

import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;

public class DateFunction extends AbstractLastFunction {

    private Function<ZonedDateTime, Integer> function;

    public DateFunction(Function<ZonedDateTime, Integer> function) {
        this.function = function;
    }

    @Override
    protected Double interGet() {
        TimePoint point = getPoint();
        Instant instant = Instant.ofEpochMilli(point.getTimestamp());
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
        return function.apply(zonedDateTime).doubleValue();
    }
}
