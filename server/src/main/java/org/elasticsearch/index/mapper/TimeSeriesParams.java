/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.function.Function;

/**
 * Utility functions for time series related mapper parameters
 */
public final class TimeSeriesParams {

    public static final String TIME_SERIES_METRIC_PARAM = "time_series_metric";
    public static final String TIME_SERIES_DIMENSION_PARAM = "time_series_dimension";

    private TimeSeriesParams() {}

    /**
     * There are various types of metric used in time-series aggregations and downsampling.
     * Each supports different field types and different calculations.
     * Two of these, the COUNTER and GAUGE apply to most numerical types, mapping a single number,
     * while the POSITION metric applies only to geo_point and therefor a pair of numbers (lat,lon).
     * To simplify code that depends on this difference, we use the parameter `scalar==true` for
     * single number metrics, and `false` for the POSITION metric.
     */
    public enum MetricType {
        GAUGE(new String[] { "max", "min", "value_count", "sum" }),
        COUNTER(new String[] { "last_value" }),
        POSITION(new String[] {}, false);

        private final String[] supportedAggs;
        private final boolean scalar;

        MetricType(String[] supportedAggs) {
            this(supportedAggs, true);
        }

        MetricType(String[] supportedAggs, boolean scalar) {
            this.supportedAggs = supportedAggs;
            this.scalar = scalar;
        }

        /** list of aggregations supported for downsampling this metric */
        public String[] supportedAggs() {
            return supportedAggs;
        }

        /** an array of metrics representing simple numerical values, like GAUGE and COUNTER */
        public static MetricType[] scalar() {
            return Arrays.stream(MetricType.values()).filter(m -> m.scalar).toArray(MetricType[]::new);
        }

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static MetricType fromString(String value) {
            for (MetricType metricType : values()) {
                if (metricType.toString().equals(value)) {
                    return metricType;
                }
            }
            throw new IllegalArgumentException("No enum constant MetricType." + value);
        }
    }

    public static FieldMapper.Parameter<MetricType> metricParam(Function<FieldMapper, MetricType> initializer, MetricType... values) {
        assert values.length > 0;
        EnumSet<MetricType> acceptedValues = EnumSet.noneOf(MetricType.class);
        acceptedValues.addAll(Arrays.asList(values));
        return FieldMapper.Parameter.restrictedEnumParam(
            TIME_SERIES_METRIC_PARAM,
            false,
            initializer,
            null,
            MetricType.class,
            acceptedValues
        ).acceptsNull();
    }

    public static FieldMapper.Parameter<Boolean> dimensionParam(Function<FieldMapper, Boolean> initializer) {
        return FieldMapper.Parameter.boolParam(TIME_SERIES_DIMENSION_PARAM, false, initializer, false);
    }

}
