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

/** The definition of a count: occurrences, bytes or document ids, recorded through a {@link Counter}. */
public final class CounterMetric implements Metric {

    private final Id id;
    private final Unit unit;
    private final String description;

    private CounterMetric(Id id, Unit unit, String description) {
        MetricRules.validate(id, unit, description);
        if (unit == Unit.TIME_NANOS) {
            throw new IllegalArgumentException("a counter cannot count [" + unit + "], declare a timer");
        }
        this.id = id;
        this.unit = unit;
        this.description = description;
    }

    /** A counter of {@code unit} named {@code id}. */
    public static CounterMetric of(Id id, Unit unit, String description) {
        return new CounterMetric(id, unit, description);
    }

    /** A counter of {@code unit} named {@code group.component.name}. */
    public static CounterMetric of(String group, String component, String name, Unit unit, String description) {
        return new CounterMetric(new Id(group, component, name), unit, description);
    }

    @Override
    public Id id() {
        return id;
    }

    @Override
    public Unit unit() {
        return unit;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CounterMetric other && id.equals(other.id) && unit == other.unit && description.equals(other.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, unit, description);
    }

    @Override
    public String toString() {
        return "counter " + id + " (" + unit + "): " + description;
    }
}
