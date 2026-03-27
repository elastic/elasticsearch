/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A single measurement from an {@link org.elasticsearch.telemetry.metric.Instrument}.
 */
public record Measurement(Number value, Map<String, Object> attributes, boolean isDouble) {
    public Measurement {
        Objects.requireNonNull(value);
    }

    public boolean isLong() {
        return isDouble == false;
    }

    public double getDouble() {
        assert isDouble;
        return value.doubleValue();
    }

    public long getLong() {
        assert isLong();
        return value.longValue();
    }

    /**
     * Add measurements with the same attributes together.  All measurements must be from the
     * same instrument.  If some measurements differ on {@link #isDouble}, @throws IllegalArgumentException
     */
    public static List<Measurement> combine(List<Measurement> measurements) {
        if (measurements == null || measurements.isEmpty()) {
            return Collections.emptyList();
        }
        boolean isDouble = measurements.get(0).isDouble;
        Map<Map<String, Object>, Number> byAttr = new HashMap<>();
        measurements.forEach(m -> {
            if (m.isDouble != isDouble) {
                throw new IllegalArgumentException("cannot combine measurements of different types");
            }
            byAttr.compute(m.attributes, (k, v) -> {
                if (v == null) return m.value;
                if (isDouble) {
                    return v.doubleValue() + m.getDouble();
                } else {
                    return v.longValue() + m.getLong();
                }
            });
        });
        return byAttr.entrySet()
            .stream()
            .map(entry -> new Measurement(entry.getValue(), entry.getKey(), isDouble))
            .collect(Collectors.toList());
    }

    /**
     * Turn a list of {@link org.elasticsearch.telemetry.Measurement} into a list of its Long or Double
     *
     * @param measurements The measurements in question
     * @param getMeasurementValue The measurement -> value (Long or Double) retrieval function
     */
    public static <T> List<T> getMeasurementValues(List<Measurement> measurements, Function<Measurement, T> getMeasurementValue) {
        List<T> measurementValues = new ArrayList<T>(measurements.size());
        for (Measurement measurement : measurements) {
            T measurementValue = getMeasurementValue.apply(measurement);
            measurementValues.add(measurementValue);
        }
        return measurementValues;
    }

    /**
     * Groups a list of {@link org.elasticsearch.telemetry.Measurement} by their attribute values
     *
     * @param measurements The measurements
     * @param getAttribute The attribute retrieval function. This must cast from Object to its return type
     * @param getMeasurementValue The measurement -> value (Long or Double) retrieval function
     */
    public static <Attr, T> Map<Attr, List<T>> groupMeasurementsByAttribute(
        List<Measurement> measurements,
        Function<Map<String, Object>, Attr> getAttribute,
        Function<Measurement, T> getMeasurementValue
    ) {
        Map<Attr, List<T>> measurementsByNode = new HashMap<>();
        for (Measurement measurement : measurements) {
            Attr attr = getAttribute.apply(measurement.attributes());
            List<T> nodeMeasurements = measurementsByNode.computeIfAbsent(attr, (k -> new ArrayList<>()));
            nodeMeasurements.add(getMeasurementValue.apply(measurement));
        }
        return measurementsByNode;
    }
}
