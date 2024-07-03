/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

class OtelHelper {
    private static final Logger logger = LogManager.getLogger(OtelHelper.class);

    static Attributes fromMap(Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Attributes.empty();
        }
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

    static Consumer<ObservableDoubleMeasurement> doubleMeasurementCallback(Supplier<Collection<DoubleWithAttributes>> observer) {
        return measurement -> {
            Collection<DoubleWithAttributes> observations;
            try {
                observations = observer.get();
            } catch (RuntimeException err) {
                assert false : "observer must not throw [" + err.getMessage() + "]";
                logger.error("doubleMeasurementCallback observer unexpected error", err);
                return;
            }
            if (observations == null) {
                return;
            }
            for (DoubleWithAttributes observation : observations) {
                if (observation != null) {
                    measurement.record(observation.value(), OtelHelper.fromMap(observation.attributes()));
                }
            }
        };
    }

    static Consumer<ObservableLongMeasurement> longMeasurementCallback(Supplier<Collection<LongWithAttributes>> observer) {
        return measurement -> {
            Collection<LongWithAttributes> observations;
            try {
                observations = observer.get();
            } catch (RuntimeException err) {
                assert false : "observer must not throw [" + err.getMessage() + "]";
                logger.error("longMeasurementCallback observer unexpected error", err);
                return;
            }
            if (observations == null) {
                return;
            }
            for (LongWithAttributes observation : observations) {
                if (observation != null) {
                    measurement.record(observation.value(), OtelHelper.fromMap(observation.attributes()));
                }
            }
        };
    }
}
