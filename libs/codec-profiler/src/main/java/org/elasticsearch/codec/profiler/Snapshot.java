/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The immutable measurements of a collector at one moment, or the sum of snapshots of disjoint work. It holds one
 * measurement per metric, ordered by {@link Metric.Id}. Each measurement carries its definition. A metric that was
 * never registered is absent, which is different from zero.
 *
 * <p>The measurement of a timer also holds its CPU time companion, so no other measurement in the snapshot can use the
 * identity of the companion.
 *
 * <p>{@link #merge} sums snapshots of disjoint work. Values and call counts saturate. Two definitions of one identity
 * fail the merge. A timer stays estimated when any part is estimated. A timer keeps its CPU time only when every part
 * that has the timer also has the CPU time. Merging a snapshot with itself counts its work twice.
 */
public final class Snapshot {

    public static final Snapshot EMPTY = new Snapshot(List.of());

    private final List<Measurement> measurements;

    private Snapshot(List<Measurement> measurements) {
        this.measurements = measurements;
    }

    /**
     * A snapshot of {@code measurements}, in any order.
     *
     * @throws IllegalArgumentException when two measurements use one identity, counting a timer's CPU time companion
     */
    public static Snapshot of(Collection<? extends Measurement> measurements) {
        final List<Measurement> sorted = new ArrayList<>(measurements);
        sorted.sort((left, right) -> left.id().compareTo(right.id()));
        final Set<Metric.Id> used = new HashSet<>();
        for (Measurement measurement : sorted) {
            claim(used, measurement.id());
            if (measurement instanceof TimerMeasurement timer && timer.definition().cpuTime() != null) {
                claim(used, timer.definition().cpuTime().id());
            }
        }
        return sorted.isEmpty() ? EMPTY : new Snapshot(Collections.unmodifiableList(sorted));
    }

    private static void claim(Set<Metric.Id> used, Metric.Id id) {
        if (used.add(id) == false) {
            throw new IllegalArgumentException("metric [" + id + "] is measured twice in one snapshot");
        }
    }

    /** The measurements, ordered by metric. */
    public List<Measurement> measurements() {
        return measurements;
    }

    /** The measurement with the identity {@code id}, or null when the metric is absent. */
    @Nullable
    public Measurement get(Metric.Id id) {
        int low = 0;
        int high = measurements.size() - 1;
        while (low <= high) {
            final int middle = (low + high) >>> 1;
            final int order = measurements.get(middle).id().compareTo(id);
            if (order == 0) {
                return measurements.get(middle);
            }
            if (order < 0) {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        return null;
    }

    /**
     * The measurement of {@code counter}, or null when it is absent.
     *
     * @throws IllegalStateException when the snapshot holds another definition of the identity
     */
    @Nullable
    public CounterMeasurement get(CounterMetric counter) {
        return (CounterMeasurement) checked(counter, get(counter.id()));
    }

    /**
     * The measurement of {@code timer}, or null when it is absent.
     *
     * @throws IllegalStateException when the snapshot holds another definition of the identity
     */
    @Nullable
    public TimerMeasurement get(TimerMetric timer) {
        return (TimerMeasurement) checked(timer, get(timer.id()));
    }

    @Nullable
    private static Measurement checked(Metric definition, @Nullable Measurement found) {
        if (found != null && found.definition().equals(definition) == false) {
            throw new IllegalStateException(
                "metric [" + definition.id() + "] is recorded as [" + found.definition() + "], not as [" + definition + "]"
            );
        }
        return found;
    }

    public boolean isEmpty() {
        return measurements.isEmpty();
    }

    /**
     * The sum of {@code parts}, which hold disjoint work.
     *
     * @throws IllegalStateException when the parts hold two definitions of one identity
     */
    public static Snapshot merge(Collection<Snapshot> parts) {
        final Map<Metric.Id, Measurement> merged = new LinkedHashMap<>();
        for (Snapshot part : parts) {
            for (Measurement measurement : part.measurements) {
                merged.merge(measurement.id(), measurement, Snapshot::plus);
            }
        }
        return of(merged.values());
    }

    private static Measurement plus(Measurement left, Measurement right) {
        if (left instanceof CounterMeasurement counter && right instanceof CounterMeasurement other) {
            return counter.plus(other);
        }
        if (left instanceof TimerMeasurement timer && right instanceof TimerMeasurement other) {
            return timer.plus(other);
        }
        throw new IllegalStateException(
            "metric [" + left.id() + "] is defined as [" + left.definition() + "] and as [" + right.definition() + "]"
        );
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Snapshot other && measurements.equals(other.measurements);
    }

    @Override
    public int hashCode() {
        return measurements.hashCode();
    }

    @Override
    public String toString() {
        return measurements.toString();
    }
}
