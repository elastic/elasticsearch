/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility functions for time series related mapper parameters
 */
public final class TimeSeriesParams {

    private TimeSeriesParams() {
    }

    public enum MetricType {
        gauge,
        counter,
        histogram,
        summary;

        /**
         * Convert string to MetricType value returning null for null or invalid string values
         */
        public static MetricType fromString(String name) {
            return name != null ? Arrays.stream(values()).filter(v -> v.name().equalsIgnoreCase(name)).findFirst().orElse(null) : null;
        }
    }

    public static FieldMapper.Parameter<String> metricParam(
        Function<FieldMapper, String> initializer,
        FieldMapper.Parameter<Boolean> hasDocValues,
        String... values
    ) {
        FieldMapper.Parameter<String> param = FieldMapper.Parameter.restrictedStringParam("metric", false, initializer, values)
            .acceptsNull();
        // We are overriding the default validator that checks for acceptable values.
        // So we must call the default validator explicitly from inside our new validator.
        // TODO: Make parameters support multiple validators
        final Consumer<String> defaultValidator = param.getValidator();
        param.setValidator(v -> {
            // Call the default validator first
            defaultValidator.accept(v);

            if (v != null && v.isEmpty() == false && hasDocValues != null && hasDocValues.getValue() == false) {
                throw new IllegalArgumentException("Field [metric] requires that [" + hasDocValues.name + "] is true");
            }
        });

        return param;
    }


}
