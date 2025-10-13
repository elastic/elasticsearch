/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.WrappedRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.ml.job.process.AbstractInitializableRunnable;
import org.elasticsearch.xpack.ml.job.process.AbstractProcessWorkerExecutorService;

import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * An {@link AbstractProcessWorkerExecutorService} where the runnables
 * are executed in priority order with a tie breaker value for requests
 * of equal priority. The tie breaker can be used to preserve insertion order.
 *
 * Order is maintained by a PriorityQueue
 */
public class PriorityProcessWorkerExecutorService extends AbstractProcessWorkerExecutorService<
    PriorityProcessWorkerExecutorService.OrderedRunnable> {

    public enum RequestPriority {
        HIGHEST,
        HIGH,
        NORMAL
    };

    /**
     * A wrapper around an {@link AbstractRunnable} which allows it to be sorted first by {@link RequestPriority}, then by a tiebreaker,
     * which in most cases will be the insertion order
     */
    protected static final class OrderedRunnable extends AbstractRunnable implements Comparable<OrderedRunnable>, WrappedRunnable {
        private final RequestPriority priority;
        private final long tieBreaker;
        private final AbstractRunnable runnable;

        protected OrderedRunnable(RequestPriority priority, long tieBreaker, AbstractRunnable runnable) {
            this.priority = priority;
            this.tieBreaker = tieBreaker;
            this.runnable = runnable;
        }

        @Override
        public int compareTo(OrderedRunnable o) {
            int p = this.priority.compareTo(o.priority);
            if (p == 0) {
                return (int) (this.tieBreaker - o.tieBreaker);
            }

            return p;
        }

        @Override
        public void onFailure(Exception e) {
            runnable.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            runnable.onRejection(e);
        }

        @Override
        protected void doRun() throws Exception {
            runnable.run();
        }

        @Override
        public boolean isForceExecution() {
            return runnable.isForceExecution();
        }

        @Override
        public void onAfter() {
            runnable.onAfter();
        }

        @Override
        public Runnable unwrap() {
            return runnable;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (OrderedRunnable) obj;
            return Objects.equals(this.priority, that.priority)
                && this.tieBreaker == that.tieBreaker
                && Objects.equals(this.runnable, that.runnable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(priority, tieBreaker, runnable);
        }

        @Override
        public String toString() {
            return "OrderedRunnable[" + "priority=" + priority + ", " + "tieBreaker=" + tieBreaker + ", " + "runnable=" + runnable + ']';
        }
    }

    private final int queueCapacity;

    /**
     * @param contextHolder the thread context holder
     * @param processName the name of the process to be used in logging
     * @param queueCapacity the capacity of the queue holding operations. If an operation is added
     *                  for execution when the queue is full a 429 error is thrown.
     */
    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public PriorityProcessWorkerExecutorService(ThreadContext contextHolder, String processName, int queueCapacity) {
        super(contextHolder, processName, queueCapacity, PriorityBlockingQueue::new);
        this.queueCapacity = queueCapacity;
    }

    /**
     * Insert a runnable into the executor queue.
     *
     * @param command The runnable
     * @param priority Request priority
     * @param tieBreaker For sorting requests of equal priority
     */
    public synchronized void executeWithPriority(AbstractInitializableRunnable command, RequestPriority priority, long tieBreaker) {
        command.init();
        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(processName + " worker service has shutdown", true);
            command.onRejection(rejected);
            notifyQueueRunnables();
            return;
        }

        // PriorityBlockingQueue is an unbounded queue so check it has not reached capacity.
        // highest priority requests are always accepted even if the queue is full
        if (queue.size() >= queueCapacity && priority != RequestPriority.HIGHEST) {
            command.onRejection(new EsRejectedExecutionException(processName + " queue is full. Unable to execute command", false));
            return;
        }

        // PriorityBlockingQueue::offer always returns true
        queue.offer(new OrderedRunnable(priority, tieBreaker, (AbstractRunnable) contextHolder.preserveContext(command)));
        if (isShutdown()) {
            // the worker shutdown during this function
            notifyQueueRunnables();
        }
    }

    @Override
    public synchronized void execute(Runnable command) {
        throw new UnsupportedOperationException("use executeWithPriority");
    }
}
