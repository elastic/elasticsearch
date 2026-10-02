/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.telemetry.apm.internal.MetricValidator;
import org.elasticsearch.telemetry.metric.DoubleAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;

import java.util.Map;
import java.util.function.Consumer;

class OtelHelper {
    private static final Logger logger = LogManager.getLogger(OtelHelper.class);

    static Attributes fromMap(String metricName, Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Attributes.empty();
        }

        MetricValidator.assertValidAttributeNames(metricName, attributes);

        var builder = Attributes.builder();
        attributes.forEach((k, v) -> {
            if (v instanceof String value) {
                builder.put(k, value);
            } else if (v instanceof Long value) {
                builder.put(k, value);
            } else if (v instanceof Integer value) {
                builder.put(k, value);
            } else if (v instanceof Byte value) {
                builder.put(k, value);
            } else if (v instanceof Short value) {
                builder.put(k, value);
            } else if (v instanceof Double value) {
                builder.put(k, value);
            } else if (v instanceof Float value) {
                builder.put(k, value);
            } else if (v instanceof Boolean value) {
                builder.put(k, value);
            } else {
                throw new IllegalArgumentException("attributes do not support value type of [" + v.getClass().getCanonicalName() + "]");
            }
        });
        return builder.build();
    }

    static Consumer<ObservableDoubleMeasurement> doubleMeasurementCallback(String metricName, Consumer<DoubleAsyncMeasurement> callback) {
        return doubleCallback(metricName, callback, false);
    }

    // Async counters skip 0-valued observations: an unobserved series is dropped by the SDK, matching the APM agent (which did
    // not emit idle counters). Gauges keep 0, since for a gauge 0 is a real value.
    static Consumer<ObservableDoubleMeasurement> doubleCounterMeasurementCallback(
        String metricName,
        Consumer<DoubleAsyncMeasurement> callback
    ) {
        return doubleCallback(metricName, callback, true);
    }

    private static Consumer<ObservableDoubleMeasurement> doubleCallback(
        String metricName,
        Consumer<DoubleAsyncMeasurement> callback,
        boolean suppressZeroValues
    ) {
        return measurement -> {
            try {
                callback.accept(new OtelDoubleAsyncMeasurement(metricName, measurement, suppressZeroValues));
            } catch (RuntimeException err) {
                assert false : "callback must not throw [" + err.getMessage() + "]";
                logger.error("doubleMeasurementCallback observer unexpected error", err);
            }
        };
    }

    static Consumer<ObservableLongMeasurement> longMeasurementCallback(String metricName, Consumer<LongAsyncMeasurement> callback) {
        return longCallback(metricName, callback, false);
    }

    static Consumer<ObservableLongMeasurement> longCounterMeasurementCallback(String metricName, Consumer<LongAsyncMeasurement> callback) {
        return longCallback(metricName, callback, true);
    }

    private static Consumer<ObservableLongMeasurement> longCallback(
        String metricName,
        Consumer<LongAsyncMeasurement> callback,
        boolean suppressZeroValues
    ) {
        return measurement -> {
            try {
                callback.accept(new OtelLongAsyncMeasurement(metricName, measurement, suppressZeroValues));
            } catch (RuntimeException err) {
                assert false : "callback must not throw [" + err.getMessage() + "]";
                logger.error("longMeasurementCallback observer unexpected error", err);
            }
        };
    }
}
