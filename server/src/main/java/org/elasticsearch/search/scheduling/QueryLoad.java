/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.util.concurrent.TaskLoadAttribution;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.atomic.LongAdder;

/**
 * The search load, CPU time and waiting of a part of a query on this node, such as a shard phase or a compute. Load and
 * CPU time come from the targets it {@link #charging wraps}. Waiting comes from the {@link QueryScheduler}.
 */
public final class QueryLoad {

    private final LongAdder nanos = new LongAdder();
    private final LongAdder cpuNanos = new LongAdder();
    private final LongAdder waitingNanos = new LongAdder();

    /**
     * A target that charges {@code target} and adds the charge to this load.
     */
    public Charging charging(TaskLoadAttribution.Target target) {
        return new Charging(this, target);
    }

    private void add(long nanos, long cpuNanos) {
        this.nanos.add(nanos);
        this.cpuNanos.add(cpuNanos);
    }

    void addWaiting(long nanos) {
        waitingNanos.add(nanos);
    }

    public long nanos() {
        return nanos.sum();
    }

    public long cpuNanos() {
        return cpuNanos.sum();
    }

    public long waitingNanos() {
        return waitingNanos.sum();
    }

    /**
     * Charges {@code target} and adds the charge to {@code load}.
     */
    public record Charging(QueryLoad load, TaskLoadAttribution.Target target) implements TaskLoadAttribution.Target {
        /**
         * The target of the task running on this thread, if it charges a load.
         */
        @Nullable
        public static Charging running() {
            return TaskLoadAttribution.target() instanceof Charging charging ? charging : null;
        }

        @Override
        public void charge(long nanos, long cpuNanos, long finishNanos) {
            load.add(nanos, cpuNanos);
            target.charge(nanos, cpuNanos, finishNanos);
        }
    }
}
