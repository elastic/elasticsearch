/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import java.util.Objects;

/**
 * The value of a counter.
 *
 * @param definition the counter's definition
 * @param value      the count, never negative, held at {@code Long.MAX_VALUE} when it would overflow
 */
public record CounterMeasurement(CounterMetric definition, long value) implements Measurement {

    public CounterMeasurement {
        Objects.requireNonNull(definition, "definition");
        if (value < 0) {
            throw new IllegalArgumentException("counter [" + definition.id() + "] cannot be negative, got [" + value + "]");
        }
    }

    /**
     * The sum of two parts of the same counter's work.
     *
     * @throws IllegalStateException when the parts were recorded under different definitions of the identity
     */
    CounterMeasurement plus(CounterMeasurement other) {
        if (definition.equals(other.definition) == false) {
            throw new IllegalStateException("metric [" + id() + "] is defined as [" + definition + "] and as [" + other.definition + "]");
        }
        return new CounterMeasurement(definition, MetricRules.add(value, other.value));
    }
}
