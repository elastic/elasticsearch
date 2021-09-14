/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Scheduler that allows to schedule one-shot and periodic commands.
 */
public interface Scheduler {

    /**
     * Create a scheduler that can be used client side. Server side, please use <code>ThreadPool.schedule</code> instead.
     *
     * Notice that if any scheduled jobs fail with an exception, these will bubble up to the uncaught exception handler where they will
     * be logged as a warning. This includes jobs started using execute, submit and schedule.
     * @param settings the settings to use
     * @param schedulerName a string that identifies the threads belonging to this scheduler
     * @return executor
     */
    static ScheduledThreadPoolExecutor initScheduler(Settings settings, String schedulerName) {
        final ScheduledThreadPoolExecutor scheduler = new SafeScheduledThreadPoolExecutor(1,
                EsExecutors.daemonThreadFactory(settings, schedulerName), new EsAbortPolicy());
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    static boolean terminate(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor, long timeout, TimeUnit timeUnit) {
        scheduledThreadPoolExecutor.shutdown();
        if (awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit)) {
            return true;
        }
        // last resort
        scheduledThreadPoolExecutor.shutdownNow();
        return awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit);
    }

    static boolean awaitTermination(final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
            final long timeout, final TimeUnit timeUnit) {
        try {
            if (scheduledThreadPoolExecutor.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Schedules a one-shot command to be run after a given delay. The command is run in the context of the calling thread.
     * The command runs on scheduler thread. Do not run blocking calls on the scheduler thread. Subclasses may allow
     * to execute on a different executor, in which case blocking calls are allowed.
     *
     * @param command the command to run
     * @param delay delay before the task executes
     * @param executor the name of the executor that has to execute this task. Ignored in the default implementation but can be used
     *                 by subclasses that support multiple executors.
     * @return a ScheduledFuture who's get will return when the task has been added to its target thread pool and throws an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture cannot interact with it.
     * @throws EsRejectedExecutionException if the task cannot be scheduled for execution
     */
    ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor);

    /**
     * Schedules a periodic action that runs on scheduler thread. Do not run blocking calls on the scheduler thread. Subclasses may allow
     * to execute on a different executor, in which case blocking calls are allowed.
     *
     * @param command the action to take
     * @param interval the delay interval
     * @param executor the name of the executor that has to execute this task. Ignored in the default implementation but can be used
     *                 by subclasses that support multiple executors.
     * @return a {@link Cancellable} that can be used to cancel the subsequent runs of the command. If the command is running, it will
     *         not be interrupted.
     */
    default Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this, (e) -> {}, (e) -> {});
    }

    /**
     * Utility method to wrap a <code>Future</code> as a <code>Cancellable</code>
     * @param future the future to wrap
     * @return a cancellable delegating to the future
     */
    static Cancellable wrapAsCancellable(Future<?> future) {
        return new CancellableAdapter(future);
    }

    /**
     * Utility method to wrap a <code>ScheduledFuture</code> as a <code>ScheduledCancellable</code>
     * @param scheduledFuture the scheduled future to wrap
     * @return a SchedulecCancellable delegating to the scheduledFuture
     */
    static ScheduledCancellable wrapAsScheduledCancellable(ScheduledFuture<?> scheduledFuture) {
        return new ScheduledCancellableAdapter(scheduledFuture);
    }


    /**
     * This interface represents an object whose execution may be cancelled during runtime.
     */
    interface Cancellable {

        /**
         * Cancel the execution of this object. This method is idempotent.
         */
        boolean cancel();

        /**
         * Check if the execution has been cancelled
         * @return true if cancelled
         */
        boolean isCancelled();
    }

    /**
     * A scheduled cancellable allow cancelling and reading the remaining delay of a scheduled task.
     */
    interface ScheduledCancellable extends Delayed, Cancellable { }

    /**
     * This class encapsulates the scheduling of a {@link Runnable} that needs to be repeated on a interval. For example, checking a value
     * for cleanup every second could be done by passing in a Runnable that can perform the check and the specified interval between
     * executions of this runnable. <em>NOTE:</em> the runnable is only rescheduled to run again after completion of the runnable.
     *
     * For this class, <i>completion</i> means that the call to {@link Runnable#run()} returned or an exception was thrown and caught. In
     * case of an exception, this class will log the exception and reschedule the runnable for its next execution. This differs from the
     * {@link ScheduledThreadPoolExecutor#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} semantics as an exception there would
     * terminate the rescheduling of the runnable.
     */
    final class ReschedulingRunnable extends AbstractRunnable implements Cancellable {

        private final Runnable runnable;
        private final TimeValue interval;
        private final String executor;
        private final Scheduler scheduler;
        private final Consumer<Exception> rejectionConsumer;
        private final Consumer<Exception> failureConsumer;

        private volatile boolean run = true;

        /**
         * Creates a new rescheduling runnable and schedules the first execution to occur after the interval specified
         *
         * @param runnable the {@link Runnable} that should be executed periodically
         * @param interval the time interval between executions
         * @param executor the executor where this runnable should be scheduled to run
         * @param scheduler the {@link Scheduler} instance to use for scheduling
         */
        ReschedulingRunnable(Runnable runnable, TimeValue interval, String executor, Scheduler scheduler,
                             Consumer<Exception> rejectionConsumer, Consumer<Exception> failureConsumer) {
            this.runnable = runnable;
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.rejectionConsumer = rejectionConsumer;
            this.failureConsumer = failureConsumer;
            scheduler.schedule(this, interval, executor);
        }

        @Override
        public boolean cancel() {
            final boolean result = run;
            run = false;
            return result;
        }

        @Override
        public boolean isCancelled() {
            return run == false;
        }

        @Override
        public void doRun() {
            // always check run here since this may have been cancelled since the last execution and we do not want to run
            if (run) {
                runnable.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            failureConsumer.accept(e);
        }

        @Override
        public void onRejection(Exception e) {
            run = false;
            rejectionConsumer.accept(e);
        }

        @Override
        public void onAfter() {
            // if this has not been cancelled reschedule it to run again
            if (run) {
                try {
                    scheduler.schedule(this, interval, executor);
                } catch (final EsRejectedExecutionException e) {
                    onRejection(e);
                }
            }
        }

        @Override
        public String toString() {
            return "ReschedulingRunnable{" +
                "runnable=" + runnable +
                ", interval=" + interval +
                '}';
        }
    }

    /**
     * This subclass ensures to properly bubble up Throwable instances of both type Error and Exception thrown in submitted/scheduled
     * tasks to the uncaught exception handler
     */
    class SafeScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
        public SafeScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, threadFactory, handler);
        }

        @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
        public SafeScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }

        @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
        public SafeScheduledThreadPoolExecutor(int corePoolSize) {
            super(corePoolSize);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (t != null) return;
            // Scheduler only allows Runnable's so we expect no checked exceptions here. If anyone uses submit directly on `this`, we
            // accept the wrapped exception in the output.
            if (r instanceof RunnableFuture && ((RunnableFuture<?>) r).isDone()) {
                // only check this if task is done, which it always is except for periodic tasks. Periodic tasks will hang on
                // RunnableFuture.get()
                ExceptionsHelper.reThrowIfNotNull(EsExecutors.rethrowErrors(r));
            }
        }
    }
}
