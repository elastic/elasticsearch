/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.Executor;

/**
 * Charges the execution time of pool tasks to the work they do. Code sets what its task works for, and the pool charges the
 * time it measured when the task completes. Only pools configured with
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
         * @param finishNanos the time it completed, from {@link System#nanoTime()}
         */
        void charge(long nanos, long finishNanos);
    }

    /** For a task that charges its time itself, so its pool charges nothing. */
    public static final Target CHARGED_BY_TASK = (nanos, finishNanos) -> {};

    /**
     * The pool task running on an {@link EsExecutors.EsThread}.
     */
    static final class Task {
        /** Whether this thread runs a tracked task. */
        boolean running;
        Target target;
        /** The time charged with {@link #charge}. */
        long chargedNanos;

        void reset(boolean running) {
            this.running = running;
            target = null;
            chargedNanos = 0;
        }
    }

    private TaskLoadAttribution() {}

    /**
     * Charges the time of the task running on this thread to {@code target}, unless the task has a target already.
     */
    public static void attribute(Target target) {
        Task task = task();
        if (task != null && task.running && task.target == null) {
            task.target = target;
        }
    }

    /**
     * Charges {@code nanos} of the task running on this thread to {@code target}, so its pool charges only the rest of the
     * task to the task's target.
     */
    public static void charge(Target target, long nanos, long finishNanos) {
        target.charge(nanos, finishNanos);
        Task task = task();
        if (task != null && task.running) {
            task.chargedNanos += nanos;
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
        task.reset(false);
        if (target == null) {
            return charged;
        }
        if (nanos > charged) {
            target.charge(nanos - charged, finishNanos);
        }
        return nanos;
    }

    /**
     * Stops tracking the task on this thread without charging it.
     */
    static void taskAbandoned() {
        Task task = task();
        if (task != null) {
            task.reset(false);
        }
    }
}
