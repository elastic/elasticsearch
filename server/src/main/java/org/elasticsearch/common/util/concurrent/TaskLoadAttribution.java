/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Charges the execution time of pool tasks to the work they do. Code sets what its task works for, and the pool charges the
 * time it measured when the task completes, with the CPU time of the thread in it. Only pools configured with
 * {@link EsExecutors.TaskTrackingConfig.Builder#attributeTaskLoad} charge.
 */
public final class TaskLoadAttribution {

    /**
     * Where the time of a task is charged.
     */
    @FunctionalInterface
    public interface Target {
        /**
         * @param nanos       the time the task ran
         * @param cpuNanos    the CPU time of its thread in that time, at most {@code nanos}
         * @param finishNanos the time it completed, from {@link System#nanoTime()}
         */
        void charge(long nanos, long cpuNanos, long finishNanos);
    }

    private static final ThreadMXBean THREAD_MX = ManagementFactory.getThreadMXBean();
    /** {@link ThreadMXBean#getCurrentThreadCpuTime} throws where it is not supported, and returns {@code -1} while disabled. */
    private static final boolean CPU_TIME_SUPPORTED = THREAD_MX.isCurrentThreadCpuTimeSupported();
    /** A CPU time that is not known. */
    private static final long UNREAD = Long.MIN_VALUE;
    /**
     * Time shorter than this is charged as CPU time without reading the clock, and the next read corrects it.
     */
    static final long SHORT_NANOS = TimeUnit.MICROSECONDS.toNanos(20);

    /**
     * The pool task running on an {@link EsExecutors.EsThread}.
     */
    static final class Task {
        /** Whether this thread runs a tracked task. */
        boolean running;
        Target target;
        /** The time charged with {@link #charge}. */
        long chargedNanos;
        /**
         * The CPU time of the thread when the clock was last read, or {@link #UNREAD} after the thread used CPU time it did not
         * charge. It is kept from task to task, since in between the thread only runs its pool's loop or waits for work.
         */
        long cpuReadNanos = UNREAD;
        /** The CPU time charged since that read without reading the clock. */
        long cpuChargedSinceRead;
        /** The CPU time of the thread, negative when it is not known. */
        LongSupplier cpuClock = TaskLoadAttribution::threadCpuNanos;

        void reset(boolean running) {
            this.running = running;
            target = null;
            chargedNanos = 0;
        }
    }

    private TaskLoadAttribution() {}

    /**
     * Charges the time of the task running on this thread to {@code target}, unless the task has a target already. Only tasks
     * with a target read the CPU time of their thread.
     */
    public static void attribute(Target target) {
        Task task = task();
        if (task != null && task.running && task.target == null) {
            task.target = target;
            if (task.cpuReadNanos == UNREAD) {
                readCpu(task);
            }
        }
    }

    /**
     * Charges {@code nanos} of the task running on this thread to {@code target}, so its pool charges only the rest of the
     * task to the task's target.
     */
    public static void charge(Target target, long nanos, long finishNanos) {
        Task task = task();
        if (task != null && task.running) {
            task.chargedNanos += nanos;
            target.charge(nanos, cpuOf(task, nanos), finishNanos);
        } else {
            target.charge(nanos, nanos, finishNanos);
        }
    }

    /**
     * Like {@link #charge(Target, long, long)} for each of {@code targets} and the {@code nanos} at the same index, splitting the
     * CPU time between them like the time.
     */
    public static void charge(List<Target> targets, long[] nanos, long finishNanos) {
        assert nanos.length >= targets.size() : nanos.length + " < " + targets.size();
        long totalNanos = 0;
        for (int i = 0; i < targets.size(); i++) {
            assert nanos[i] >= 0 : nanos[i];
            totalNanos += nanos[i];
        }
        Task task = task();
        long cpuNanos;
        if (task != null && task.running) {
            task.chargedNanos += totalNanos;
            cpuNanos = cpuOf(task, totalNanos);
        } else {
            cpuNanos = totalNanos;
        }
        // rounds the cumulative share, so the shares add up to cpuNanos, but never more than the time of each
        long nanosSoFar = 0;
        long cpuCharged = 0;
        for (int i = 0; i < targets.size(); i++) {
            if (nanos[i] > 0) {
                nanosSoFar += nanos[i];
                long cpu = Math.min(Math.round((double) cpuNanos * nanosSoFar / totalNanos) - cpuCharged, nanos[i]);
                cpuCharged += cpu;
                targets.get(i).charge(nanos[i], cpu, finishNanos);
            }
        }
    }

    /**
     * Whether the time of the task running on this thread is charged by its pool.
     */
    public static boolean chargedByPool() {
        Task task = task();
        return task != null && task.running;
    }

    /**
     * An executor whose tasks are charged to {@code target}.
     */
    public static Executor charging(Executor executor, Target target) {
        return task -> executor.execute(task instanceof AbstractRunnable runnable ? new Charging(runnable, target) : () -> {
            attribute(target);
            task.run();
        });
    }

    /**
     * Runs {@code task} charged to {@code target}, forwarding its completion so that it completes once.
     */
    private static final class Charging extends AbstractRunnable {
        private final AbstractRunnable task;
        private final Target target;

        Charging(AbstractRunnable task, Target target) {
            this.task = task;
            this.target = target;
        }

        @Override
        protected void doRun() throws Exception {
            attribute(target);
            task.doRun();
        }

        @Override
        public void onFailure(Exception e) {
            task.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            task.onRejection(e);
        }

        @Override
        public void onAfter() {
            task.onAfter();
        }

        @Override
        public boolean isForceExecution() {
            return task.isForceExecution();
        }

        @Override
        public String toString() {
            return task.toString();
        }
    }

    /**
     * The pool task running on this thread, or {@code null} if it is not a pool thread.
     */
    private static Task task() {
        return Thread.currentThread() instanceof EsExecutors.EsThread thread ? thread.loadTask : null;
    }

    /**
     * Starts tracking a task on this thread, if it is a thread that can track one.
     */
    static void taskStarted() {
        Task task = task();
        if (task != null) {
            task.reset(true);
        }
    }

    /**
     * Charges the task that completed on this thread to its target.
     *
     * @return the part of its time that was charged
     */
    static long taskCompleted(long nanos, long finishNanos) {
        Task task = task();
        if (task == null || task.running == false) {
            return 0;
        }
        Target target = task.target;
        long charged = Math.min(task.chargedNanos, nanos);
        if (target == null) {
            task.cpuReadNanos = UNREAD;
        } else if (nanos > charged) {
            target.charge(nanos - charged, cpuOf(task, nanos - charged), finishNanos);
        }
        task.reset(false);
        return target == null ? charged : nanos;
    }

    /**
     * The CPU time in {@code nanos} of the task on this thread: all of it when it is short or not known, else what the thread used
     * since its last read less what was charged since, at most {@code nanos}.
     */
    private static long cpuOf(Task task, long nanos) {
        if (task.cpuReadNanos == UNREAD) {
            return nanos;
        }
        if (nanos < SHORT_NANOS) {
            task.cpuChargedSinceRead += nanos;
            return nanos;
        }
        long lastRead = task.cpuReadNanos;
        long chargedSinceRead = task.cpuChargedSinceRead;
        readCpu(task);
        return Math.min(Math.max(task.cpuReadNanos - lastRead - chargedSinceRead, 0), nanos);
    }

    private static void readCpu(Task task) {
        long cpuNanos = task.cpuClock.getAsLong();
        task.cpuReadNanos = cpuNanos < 0 ? UNREAD : cpuNanos;
        task.cpuChargedSinceRead = 0;
    }

    private static long threadCpuNanos() {
        return CPU_TIME_SUPPORTED ? THREAD_MX.getCurrentThreadCpuTime() : -1;
    }

    /**
     * Stops tracking the task on this thread without charging it.
     */
    static void taskAbandoned() {
        Task task = task();
        if (task != null) {
            task.cpuReadNanos = UNREAD;
            task.reset(false);
        }
    }
}
