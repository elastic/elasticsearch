/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.function.LongConsumer;

/**
 * Utility for measuring per-thread CPU time in format readers.
 * <p>
 * Call {@link #currentNanos()} before and after a timed region.  A return
 * value of {@code -1} means the JVM does not support per-thread CPU timing
 * (e.g. GraalVM Native Image); callers must guard with {@code if (start >= 0)}
 * before accumulating a delta.
 */
public final class ThreadCpuTimer {

    private static final ThreadMXBean THREAD_MX = ManagementFactory.getThreadMXBean();

    private ThreadCpuTimer() {}

    /**
     * Returns the CPU time consumed by the current thread in nanoseconds, or
     * {@code -1} if {@link ThreadMXBean#isCurrentThreadCpuTimeSupported()} is
     * {@code false} or CPU-time collection has been disabled at runtime.
     */
    public static long currentNanos() {
        return THREAD_MX.getCurrentThreadCpuTime();
    }

    /**
     * Returns the CPU nanos elapsed since {@code startCpuNanos}, clamped to zero.
     * Callers must still guard with {@code if (startCpuNanos >= 0)} before calling this.
     */
    public static long elapsedNanos(long startCpuNanos) {
        return Math.max(0L, THREAD_MX.getCurrentThreadCpuTime() - startCpuNanos);
    }

    /**
     * Wraps {@code iter} so that when it is closed, {@code object.asyncCpuNanos()} is passed to
     * {@code accumulator}. Used by format readers to collect CPU time spent on background threads
     * (e.g. the GCS executor) that is invisible to the producer thread's {@link ThreadMXBean}.
     */
    public static CloseableIterator<Page> withAsyncCpuOnClose(
        CloseableIterator<Page> iter,
        StorageObject object,
        LongConsumer accumulator
    ) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Page next() {
                return iter.next();
            }

            @Override
            public SubscribableListener<Void> waitForReady() {
                return iter.waitForReady();
            }

            @Override
            public void close() throws IOException {
                try {
                    iter.close();
                } finally {
                    accumulator.accept(object.asyncCpuNanos());
                }
            }
        };
    }
}
