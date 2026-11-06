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
import java.util.OptionalLong;

/**
 * The measurement of a timer: its elapsed time and, when measured, the CPU time of the same calls, with one pair of call
 * counts for both. A CPU time thus never exists without the elapsed time of its calls.
 *
 * @param definition   the timer's definition
 * @param elapsedNanos the elapsed time of the calls, an estimate when {@code estimated}
 * @param cpuNanos     the thread CPU time of the same measured calls, estimated the same way; empty when the
 *                     definition has no CPU time companion, when the collector did not measure CPU time, or when a
 *                     CPU reading failed
 * @param sampledCalls the number of measured calls
 * @param totalCalls   the number of started calls, including calls that threw
 * @param estimated    whether a call went unmeasured, so the times are estimates; true when
 *                     {@code sampledCalls < totalCalls}, and kept true through merges even when saturated counts
 *                     no longer show it
 */
public record TimerMeasurement(
    TimerMetric definition,
    long elapsedNanos,
    OptionalLong cpuNanos,
    long sampledCalls,
    long totalCalls,
    boolean estimated
) implements Measurement {

    public TimerMeasurement {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(cpuNanos, "cpuNanos");
        if (elapsedNanos < 0 || sampledCalls < 0 || totalCalls < 0 || (cpuNanos.isPresent() && cpuNanos.getAsLong() < 0)) {
            throw new IllegalArgumentException("timer [" + definition.id() + "] cannot be negative");
        }
        if (sampledCalls > totalCalls) {
            throw new IllegalArgumentException(
                "timer [" + definition.id() + "] measured [" + sampledCalls + "] of only [" + totalCalls + "] calls"
            );
        }
        if (sampledCalls < totalCalls && estimated == false) {
            throw new IllegalArgumentException("timer [" + definition.id() + "] left calls unmeasured, so its value is an estimate");
        }
        if (cpuNanos.isPresent() && definition.cpuTime() == null) {
            throw new IllegalArgumentException("timer [" + definition.id() + "] has no CPU time companion to report");
        }
    }

    /**
     * The sum of two parts of the work of one timer. The sum has a CPU time only when both parts have one, because a
     * partial CPU sum would cover only some of the calls.
     *
     * @throws IllegalStateException when the parts were recorded under different definitions of the identity
     */
    TimerMeasurement plus(TimerMeasurement other) {
        if (definition.equals(other.definition) == false) {
            throw new IllegalStateException("metric [" + id() + "] is defined as [" + definition + "] and as [" + other.definition + "]");
        }
        final OptionalLong cpu = cpuNanos.isPresent() && other.cpuNanos.isPresent()
            ? OptionalLong.of(MetricRules.add(cpuNanos.getAsLong(), other.cpuNanos.getAsLong()))
            : OptionalLong.empty();
        return new TimerMeasurement(
            definition,
            MetricRules.add(elapsedNanos, other.elapsedNanos),
            cpu,
            MetricRules.add(sampledCalls, other.sampledCalls),
            MetricRules.add(totalCalls, other.totalCalls),
            estimated || other.estimated
        );
    }
}
