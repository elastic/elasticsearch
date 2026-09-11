/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates wall-clock and CPU time for external data-source reads at the operator level.
 *
 */
public class ExternalReadCounters {

    /** A shared instance which does not do any counting. Use where no counters are needed (tests, benchmarks). */
    public static final ExternalReadCounters NOOP = new ExternalReadCounters() {
        @Override
        public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work, boolean measureWallTime) throws E {
            return work.get();
        }

        @Override
        void add(long readNanos, long readCpuNanos) {}
    };

    private final AtomicLong readNanosAcc = new AtomicLong();
    private final AtomicLong readCpuNanosAcc = new AtomicLong();

    private final ThreadLocal<Boolean> samplingCpu = new ThreadLocal<>();

    // for deserializations
    public static ExternalReadCounters fromCounters(long readNanos, long readCpuNanos) {
        ExternalReadCounters cnt = new ExternalReadCounters();
        cnt.add(readNanos, readCpuNanos);
        return cnt;
    }

    public <E extends Exception> void meteredCpu(CheckedRunnable<E> work) throws E {
        meteredCpu(() -> {
            work.run();
            return null;
        });
    }

    public <E extends Exception> void meteredCpu(CheckedRunnable<E> work, boolean measureWallTime) throws E {
        meteredCpu(() -> {
            work.run();
            return null;
        }, measureWallTime);
    }

    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
        return meteredCpu(work, true);
    }

    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work, boolean measureWallTime) throws E {
        if (Boolean.TRUE.equals(samplingCpu.get())) {
            return work.get();
        }
        long startCpuNanos = ThreadCpuTimer.currentNanos();
        long startNanos = System.nanoTime();
        samplingCpu.set(Boolean.TRUE);
        try {
            return work.get();
        } finally {
            if (measureWallTime) {
                readNanosAcc.addAndGet(System.nanoTime() - startNanos);
            }
            if (startCpuNanos >= 0) {
                readCpuNanosAcc.addAndGet(ThreadCpuTimer.elapsedNanos(startCpuNanos));
            }
            samplingCpu.remove();
        }
    }

    public long readNanos() {
        return readNanosAcc.get();
    }

    public long readCpuNanos() {
        return readCpuNanosAcc.get();
    }

    /** Directly adds to both accumulators. For use in tests. */
    void add(long readNanos, long readCpuNanos) {
        readNanosAcc.addAndGet(readNanos);
        readCpuNanosAcc.addAndGet(readCpuNanos);
    }
}
