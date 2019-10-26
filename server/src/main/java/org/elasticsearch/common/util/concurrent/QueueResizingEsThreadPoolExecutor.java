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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * An extension to thread pool executor, which automatically adjusts the queue size of the
 * {@code ResizableBlockingQueue} according to Little's Law.
 */
public final class QueueResizingEsThreadPoolExecutor extends EsThreadPoolExecutor {

    // This is a random starting point alpha. TODO: revisit this with actual testing and/or make it configurable
    public static double EWMA_ALPHA = 0.3;

    private static final Logger logger = LogManager.getLogger(QueueResizingEsThreadPoolExecutor.class);
    // The amount the queue size is adjusted by for each calcuation
    private static final int QUEUE_ADJUSTMENT_AMOUNT = 50;

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ResizableBlockingQueue<Runnable> workQueue;
    private final int tasksPerFrame;
    private final int minQueueSize;
    private final int maxQueueSize;
    private final long targetedResponseTimeNanos;
    private final ExponentiallyWeightedMovingAverage executionEWMA;

    private final AtomicLong totalTaskNanos = new AtomicLong(0);
    private final AtomicInteger taskCount = new AtomicInteger(0);

    private long startNs;

    QueueResizingEsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                      ResizableBlockingQueue<Runnable> workQueue, int minQueueSize, int maxQueueSize,
                                      Function<Runnable, WrappedRunnable> runnableWrapper, final int tasksPerFrame,
                                      TimeValue targetedResponseTime, ThreadFactory threadFactory, XRejectedExecutionHandler handler,
                                      ThreadContext contextHolder) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit,
                workQueue, threadFactory, handler, contextHolder);
        this.runnableWrapper = runnableWrapper;
        this.workQueue = workQueue;
        this.tasksPerFrame = tasksPerFrame;
        this.startNs = System.nanoTime();
        this.minQueueSize = minQueueSize;
        this.maxQueueSize = maxQueueSize;
        this.targetedResponseTimeNanos = targetedResponseTime.getNanos();
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
        logger.debug(
                "thread pool [{}] will adjust queue by [{}] when determining automatic queue size", getName(), QUEUE_ADJUSTMENT_AMOUNT);
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
     * Calculate task rate (λ), for a fixed number of tasks and time it took those tasks to be measured
     *
     * @param totalNumberOfTasks total number of tasks that were measured
     * @param totalFrameTaskNanos nanoseconds during which the tasks were received
     * @return the rate of tasks in the system
     */
    static double calculateLambda(final int totalNumberOfTasks, final long totalFrameTaskNanos) {
        assert totalFrameTaskNanos > 0 : "cannot calculate for instantaneous tasks, got: " + totalFrameTaskNanos;
        assert totalNumberOfTasks > 0 : "cannot calculate for no tasks, got: " + totalNumberOfTasks;
        // There is no set execution time, instead we adjust the time window based on the
        // number of completed tasks, so there is no background thread required to update the
        // queue size at a regular interval. This means we need to calculate our λ by the
        // total runtime, rather than a fixed interval.

        // λ = total tasks divided by measurement time
        return (double) totalNumberOfTasks / totalFrameTaskNanos;
    }

    /**
     * Calculate Little's Law (L), which is the "optimal" queue size for a particular task rate (lambda) and targeted response time.
     *
     * @param lambda the arrival rate of tasks in nanoseconds
     * @param targetedResponseTimeNanos nanoseconds for the average targeted response rate of requests
     * @return the optimal queue size for the give task rate and targeted response time
     */
    static int calculateL(final double lambda, final long targetedResponseTimeNanos) {
        assert targetedResponseTimeNanos > 0 : "cannot calculate for instantaneous requests";
        // L = λ * W
        return Math.toIntExact((long)(lambda * targetedResponseTimeNanos));
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
        return workQueue.size();
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
        final long taskNanos = timedRunnable.getTotalNanos();
        final boolean failedOrRejected = timedRunnable.getFailedOrRejected();
        final long totalNanos = totalTaskNanos.addAndGet(taskNanos);

        final long taskExecutionNanos = timedRunnable.getTotalExecutionNanos();
        assert taskExecutionNanos >= 0 || (failedOrRejected && taskExecutionNanos == -1) :
            "expected task to always take longer than 0 nanoseconds or have '-1' failure code, got: " + taskExecutionNanos +
                ", failedOrRejected: " + failedOrRejected;
        if (taskExecutionNanos != -1) {
            // taskExecutionNanos may be -1 if the task threw an exception
            executionEWMA.addValue(taskExecutionNanos);
        }

        if (taskCount.incrementAndGet() == this.tasksPerFrame) {
            final long endTimeNs = System.nanoTime();
            final long totalRuntime = endTimeNs - this.startNs;
            // Reset the start time for all tasks. At first glance this appears to need to be
            // volatile, since we are reading from a different thread when it is set, but it
            // is protected by the taskCount memory barrier.
            // See: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/atomic/package-summary.html
            startNs = endTimeNs;

            // Calculate the new desired queue size
            try {
                final double lambda = calculateLambda(tasksPerFrame, Math.max(totalNanos, 1L));
                final int desiredQueueSize = calculateL(lambda, targetedResponseTimeNanos);
                final int oldCapacity = workQueue.capacity();

                if (logger.isDebugEnabled()) {
                    final long avgTaskTime = totalNanos / tasksPerFrame;
                    logger.debug("[{}]: there were [{}] tasks in [{}], avg task time [{}], EWMA task execution [{}], " +
                                    "[{} tasks/s], optimal queue is [{}], current capacity [{}]",
                            getName(),
                            tasksPerFrame,
                            TimeValue.timeValueNanos(totalRuntime),
                            TimeValue.timeValueNanos(avgTaskTime),
                            TimeValue.timeValueNanos((long)executionEWMA.getAverage()),
                            String.format(Locale.ROOT, "%.2f", lambda * TimeValue.timeValueSeconds(1).nanos()),
                            desiredQueueSize,
                            oldCapacity);
                }

                // Adjust the queue size towards the desired capacity using an adjust of
                // QUEUE_ADJUSTMENT_AMOUNT (either up or down), keeping in mind the min and max
                // values the queue size can have.
                final int newCapacity =
                        workQueue.adjustCapacity(desiredQueueSize, QUEUE_ADJUSTMENT_AMOUNT, minQueueSize, maxQueueSize);
                if (oldCapacity != newCapacity && logger.isDebugEnabled()) {
                    logger.debug("adjusted [{}] queue size by [{}], old capacity: [{}], new capacity: [{}]", getName(),
                            newCapacity > oldCapacity ? QUEUE_ADJUSTMENT_AMOUNT : -QUEUE_ADJUSTMENT_AMOUNT,
                            oldCapacity, newCapacity);
                }
            } catch (ArithmeticException e) {
                // There was an integer overflow, so just log about it, rather than adjust the queue size
                logger.warn(() -> new ParameterizedMessage(
                                "failed to calculate optimal queue size for [{}] thread pool, " +
                                "total frame time [{}ns], tasks [{}], task execution time [{}ns]",
                                getName(), totalRuntime, tasksPerFrame, totalNanos),
                        e);
            } finally {
                // Finally, decrement the task count and time back to their starting values. We
                // do this at the end so there is no concurrent adjustments happening. We also
                // decrement them instead of resetting them back to zero, as resetting them back
                // to zero causes operations that came in during the adjustment to be uncounted
                int tasks = taskCount.addAndGet(-this.tasksPerFrame);
                assert tasks >= 0 : "tasks should never be negative, got: " + tasks;

                if (tasks >= this.tasksPerFrame) {
                    // Start over, because we can potentially reach a "never adjusting" state,
                    //
                    // consider the following:
                    // - If the frame window is 10, and there are 10 tasks, then an adjustment will begin. (taskCount == 10)
                    // - Prior to the adjustment being done, 15 more tasks come in, the taskCount is now 25
                    // - Adjustment happens and we decrement the tasks by 10, taskCount is now 15
                    // - Since taskCount will now be incremented forever, it will never be 10 again,
                    //   so there will be no further adjustments
                    logger.debug(
                            "[{}]: too many incoming tasks while queue size adjustment occurs, resetting measurements to 0", getName());
                    totalTaskNanos.getAndSet(1);
                    taskCount.getAndSet(0);
                    startNs = System.nanoTime();
                } else {
                    // Do a regular adjustment
                    totalTaskNanos.addAndGet(-totalNanos);
                }
            }
        }
    }

    @Override
    protected void appendThreadPoolExecutorDetails(StringBuilder sb) {
        sb.append("min queue capacity = ").append(minQueueSize).append(", ");
        sb.append("max queue capacity = ").append(maxQueueSize).append(", ");
        sb.append("frame size = ").append(tasksPerFrame).append(", ");
        sb.append("targeted response rate = ").append(TimeValue.timeValueNanos(targetedResponseTimeNanos)).append(", ");
        sb.append("task execution EWMA = ").append(TimeValue.timeValueNanos((long) executionEWMA.getAverage())).append(", ");
        sb.append("adjustment amount = ").append(QUEUE_ADJUSTMENT_AMOUNT).append(", ");
    }

}
