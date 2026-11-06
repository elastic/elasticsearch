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

import java.util.Objects;

/**
 * The definition of a timer, recorded through a {@link Timer}: the elapsed time of calls in nanoseconds, measured as its
 * {@link TimingPolicy} decides, and optionally the thread CPU time of the same measured calls under a companion
 * identity.
 *
 * <p>The companion is part of the definition, so all readers that ask for the timer agree on it before the first call.
 * A collector that measures CPU time reads it for every measured call. A collector that does not measure CPU time
 * reports no CPU time.
 */
public final class TimerMetric implements Metric {

    private final Id id;
    private final TimingPolicy policy;
    private final String description;
    @Nullable
    private final CpuTime cpuTime;

    private TimerMetric(Id id, TimingPolicy policy, String description, @Nullable CpuTime cpuTime) {
        MetricRules.validate(id, Unit.TIME_NANOS, description);
        this.id = id;
        this.policy = Objects.requireNonNull(policy, "policy");
        this.description = description;
        if (cpuTime != null && cpuTime.id().equals(id)) {
            throw new IllegalArgumentException("metric [" + id + "] cannot be the CPU time of itself");
        }
        this.cpuTime = cpuTime;
    }

    /** An elapsed-time timer named {@code id} that measures calls with {@code policy}. */
    public static TimerMetric of(Id id, TimingPolicy policy, String description) {
        return new TimerMetric(id, policy, description, null);
    }

    /** An elapsed-time timer named {@code group.component.name} that measures calls with {@code policy}. */
    public static TimerMetric of(String group, String component, String name, TimingPolicy policy, String description) {
        return new TimerMetric(new Id(group, component, name), policy, description, null);
    }

    /**
     * A new definition: this timer with a thread CPU time companion named {@code name} in the same group and
     * component. The companion uses the policy and the calls of this timer.
     *
     * @throws IllegalStateException when this timer already has a companion
     */
    public TimerMetric withCpuTime(String name, String cpuDescription) {
        if (cpuTime != null) {
            throw new IllegalStateException("metric [" + id + "] already has the CPU time companion [" + cpuTime.id() + "]");
        }
        return new TimerMetric(id, policy, description, new CpuTime(new Id(id.group(), id.component(), name), cpuDescription));
    }

    @Override
    public Id id() {
        return id;
    }

    @Override
    public Unit unit() {
        return Unit.TIME_NANOS;
    }

    @Override
    public String description() {
        return description;
    }

    /** Which calls the timer measures. */
    public TimingPolicy policy() {
        return policy;
    }

    /** The CPU time companion, or null when the timer has none. */
    @Nullable
    public CpuTime cpuTime() {
        return cpuTime;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimerMetric other
            && id.equals(other.id)
            && policy == other.policy
            && description.equals(other.description)
            && Objects.equals(cpuTime, other.cpuTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, policy, description, cpuTime);
    }

    @Override
    public String toString() {
        return "timer " + id + " (" + policy + (cpuTime == null ? "" : ", CPU time as " + cpuTime.id()) + "): " + description;
    }

    /** The thread CPU time of a timer's calls: its own identity and description, with the timer's policy and calls. */
    public static final class CpuTime {
        private final Id id;
        private final String description;

        CpuTime(Id id, String description) {
            MetricRules.validate(id, Unit.TIME_NANOS, description);
            this.id = id;
            this.description = description;
        }

        /** What the CPU time is called; a time, so its name ends in {@code _time_nanos}. */
        public Id id() {
            return id;
        }

        /** What the CPU time means. */
        public String description() {
            return description;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CpuTime other && id.equals(other.id) && description.equals(other.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, description);
        }

        @Override
        public String toString() {
            return id + ": " + description;
        }
    }
}
