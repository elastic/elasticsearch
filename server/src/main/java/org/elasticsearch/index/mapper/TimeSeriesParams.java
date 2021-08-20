/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.function.Function;

/**
 * Utility functions for time series related mapper parameters
 */
public final class TimeSeriesParams {

    public static final String TIME_SERIES_METRIC_PARAM = "time_series_metric";

    private TimeSeriesParams() {
    }

    public enum MetricType {
        gauge,
        counter,
        histogram,
        summary;

        /**
         * Convert string to MetricType value returning null for null values
         */
        public static MetricType fromString(String name) {
            return name != null ? valueOf(name) : null;
        }
    }

    public static FieldMapper.Parameter<String> metricParam(Function<FieldMapper, String> initializer, String... values) {
        return FieldMapper.Parameter.restrictedStringParam(TIME_SERIES_METRIC_PARAM, false, initializer, values).acceptsNull();
    }

}
