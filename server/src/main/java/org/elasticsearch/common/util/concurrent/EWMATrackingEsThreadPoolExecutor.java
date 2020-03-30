/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * An extension to thread pool executor, which tracks the exponentially weighted moving average of the task execution time.
 */
public final class EWMATrackingEsThreadPoolExecutor extends EsThreadPoolExecutor {

    // This is a random starting point alpha. TODO: revisit this with actual testing and/or make it configurable
    public static double EWMA_ALPHA = 0.3;

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;

    EWMATrackingEsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                     BlockingQueue<Runnable> workQueue, Function<Runnable, WrappedRunnable> runnableWrapper,
                                     ThreadFactory threadFactory, XRejectedExecutionHandler handler, ThreadContext contextHolder) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit,
            workQueue, threadFactory, handler, contextHolder);
        this.runnableWrapper = runnableWrapper;
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
    }

    @Override
    protected Runnable wrapRunnable(Runnable command) {
        return super.wrapRunnable(this.runnableWrapper.apply(command));
    }

    @Override
    protected Runnable unwrap(Runnable runnable) {
        final Runnable unwrapped = super.unwrap(runnable);
        if (unwrapped instanceof WrappedRunnable) {
            return ((WrappedRunnable) unwrapped).unwrap();
        } else {
            return unwrapped;
        }
    }

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    public double getTaskExecutionEWMA() {
        return executionEWMA.getAverage();
    }

    /**
     * Returns the current queue size (operations that are queued)
     */
    public int getCurrentQueueSize() {
        return getQueue().size();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // A task has been completed, it has left the building. We should now be able to get the
        // total time as a combination of the time in the queue and time spent running the task. We
        // only want runnables that did not throw errors though, because they could be fast-failures
        // that throw off our timings, so only check when t is null.
        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        final boolean failedOrRejected = timedRunnable.getFailedOrRejected();
        final long taskExecutionNanos = timedRunnable.getTotalExecutionNanos();
        assert taskExecutionNanos >= 0 || (failedOrRejected && taskExecutionNanos == -1) :
            "expected task to always take longer than 0 nanoseconds or have '-1' failure code, got: " + taskExecutionNanos +
                ", failedOrRejected: " + failedOrRejected;
        if (taskExecutionNanos != -1) {
            // taskExecutionNanos may be -1 if the task threw an exception
            executionEWMA.addValue(taskExecutionNanos);
        }
    }

    @Override
    protected void appendThreadPoolExecutorDetails(StringBuilder sb) {
        sb.append("task execution EWMA = ").append(TimeValue.timeValueNanos((long) executionEWMA.getAverage())).append(", ");
    }

}
