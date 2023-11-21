/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            byAttr.compute(
                m.attributes,
                (k, v) -> (v == null) ? m.value : isDouble ? v.doubleValue() + m.getDouble() : v.longValue() + m.getLong()
            );
        });
        return byAttr.entrySet()
            .stream()
            .map(entry -> new Measurement(entry.getValue(), entry.getKey(), isDouble))
            .collect(Collectors.toList());
    }
}
