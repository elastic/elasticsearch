/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * Task behavior for {@link BulkByScrollTask} that does the actual work of querying and indexing
 */
public class WorkerBulkByScrollTaskState implements SuccessfullyProcessed {

    private static final Logger logger = LogManager.getLogger(WorkerBulkByScrollTaskState.class);

    /**
     * Maximum wait time allowed for throttling.
     */
    private static final TimeValue MAX_THROTTLE_WAIT_TIME = TimeValue.timeValueHours(1);

    private final BulkByScrollTask task;

    /**
     * The id of the slice that this worker is processing or {@code null} if this task isn't for a sliced request.
     */
    private final Integer sliceId;

    /**
     * The total number of documents this request will process. 0 means we don't yet know or, possibly, there are actually 0 documents
     * to process. Its ok that these have the same meaning because any request with 0 actual documents should be quite short lived.
     */
    private final AtomicLong total = new AtomicLong(0);
    private final AtomicLong updated = new AtomicLong(0);
    private final AtomicLong created = new AtomicLong(0);
    private final AtomicLong deleted = new AtomicLong(0);
    private final AtomicLong noops = new AtomicLong(0);
    private final AtomicInteger batch = new AtomicInteger(0);
    private final AtomicLong versionConflicts = new AtomicLong(0);
    private final AtomicLong bulkRetries = new AtomicLong(0);
    private final AtomicLong searchRetries = new AtomicLong(0);
    private final AtomicLong throttledNanos = new AtomicLong();

    /**
     * The number of requests per second to which to throttle the request that this task represents. The other variables are all AtomicXXX
     * style variables but there isn't an AtomicFloat so we just use a volatile.
     */
    private volatile float requestsPerSecond;

    /**
     * Reference to any the last delayed prepareBulkRequest call. Used during rethrottling and canceling to reschedule the request.
     */
    private final AtomicReference<DelayedPrepareBulkRequest> delayedPrepareBulkRequestReference = new AtomicReference<>();

    public WorkerBulkByScrollTaskState(BulkByScrollTask task, Integer sliceId, float requestsPerSecond) {
        this.task = task;
        this.sliceId = sliceId;
        setRequestsPerSecond(requestsPerSecond);
    }

    public BulkByScrollTask.Status getStatus() {
        return new BulkByScrollTask.Status(
            sliceId,
            total.get(),
            updated.get(),
            created.get(),
            deleted.get(),
            batch.get(),
            versionConflicts.get(),
            noops.get(),
            bulkRetries.get(),
            searchRetries.get(),
            timeValueNanos(throttledNanos.get()),
            getRequestsPerSecond(),
            task.getReasonCancelled(),
            throttledUntil()
        );
    }

    public void handleCancel() {
        // Drop the throttle to 0, immediately rescheduling any throttle operation so it will wake up and cancel itself.
        rethrottle(Float.POSITIVE_INFINITY);
    }

    public void setTotal(long totalHits) {
        total.set(totalHits);
    }

    public void countBatch() {
        batch.incrementAndGet();
    }

    public void countNoop() {
        noops.incrementAndGet();
    }

    @Override
    public long getCreated() {
        return created.get();
    }

    public void countCreated() {
        created.incrementAndGet();
    }

    @Override
    public long getUpdated() {
        return updated.get();
    }

    public void countUpdated() {
        updated.incrementAndGet();
    }

    @Override
    public long getDeleted() {
        return deleted.get();
    }

    public void countDeleted() {
        deleted.incrementAndGet();
    }

    public void countVersionConflict() {
        versionConflicts.incrementAndGet();
    }

    public void countBulkRetry() {
        bulkRetries.incrementAndGet();
    }

    public void countSearchRetry() {
        searchRetries.incrementAndGet();
    }

    float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    TimeValue throttledUntil() {
        DelayedPrepareBulkRequest delayed = delayedPrepareBulkRequestReference.get();
        if (delayed == null) {
            return timeValueNanos(0);
        }
        if (delayed.scheduled == null) {
            return timeValueNanos(0);
        }
        return timeValueNanos(max(0, delayed.scheduled.getDelay(TimeUnit.NANOSECONDS)));
    }

    /**
     * Schedule prepareBulkRequestRunnable to run after some delay. This is where throttling plugs into reindexing so the request can be
     * rescheduled over and over again.
     */
    public void delayPrepareBulkRequest(
        ThreadPool threadPool,
        long lastBatchStartTimeNS,
        int lastBatchSize,
        AbstractRunnable prepareBulkRequestRunnable
    ) {
        // Synchronize so we are less likely to schedule the same request twice.
        synchronized (delayedPrepareBulkRequestReference) {
            TimeValue delay = throttleWaitTime(lastBatchStartTimeNS, System.nanoTime(), lastBatchSize);
            logger.debug("[{}]: preparing bulk request for [{}]", task.getId(), delay);
            try {
                delayedPrepareBulkRequestReference.set(
                    new DelayedPrepareBulkRequest(threadPool, getRequestsPerSecond(), delay, new RunOnce(prepareBulkRequestRunnable))
                );
            } catch (EsRejectedExecutionException e) {
                prepareBulkRequestRunnable.onRejection(e);
            }
        }
    }

    public TimeValue throttleWaitTime(long lastBatchStartTimeNS, long nowNS, int lastBatchSize) {
        long earliestNextBatchStartTime = nowNS + (long) perfectlyThrottledBatchTime(lastBatchSize);
        long waitTime = min(MAX_THROTTLE_WAIT_TIME.nanos(), max(0, earliestNextBatchStartTime - System.nanoTime()));
        return timeValueNanos(waitTime);
    }

    /**
     * How many nanoseconds should a batch of lastBatchSize have taken if it were perfectly throttled? Package private for testing.
     */
    float perfectlyThrottledBatchTime(int lastBatchSize) {
        if (requestsPerSecond == Float.POSITIVE_INFINITY) {
            return 0;
        }
        // requests
        // ------------------- == seconds
        // request per seconds
        float targetBatchTimeInSeconds = lastBatchSize / requestsPerSecond;
        // nanoseconds per seconds * seconds == nanoseconds
        return TimeUnit.SECONDS.toNanos(1) * targetBatchTimeInSeconds;
    }

    private void setRequestsPerSecond(float requestsPerSecond) {
        if (requestsPerSecond <= 0) {
            throw new IllegalArgumentException("requests per second must be more than 0 but was [" + requestsPerSecond + "]");
        }
        this.requestsPerSecond = requestsPerSecond;
    }

    /**
     * Apply {@code newRequestsPerSecond} as the new rate limit for this task's search requests
     */
    public void rethrottle(float newRequestsPerSecond) {
        synchronized (delayedPrepareBulkRequestReference) {
            logger.debug("[{}]: rethrottling to [{}] requests per second", task.getId(), newRequestsPerSecond);
            setRequestsPerSecond(newRequestsPerSecond);

            DelayedPrepareBulkRequest delayedPrepareBulkRequest = this.delayedPrepareBulkRequestReference.get();
            if (delayedPrepareBulkRequest == null) {
                // No request has been queued so nothing to reschedule.
                logger.debug("[{}]: skipping rescheduling because there is no scheduled task", task.getId());
                return;
            }

            this.delayedPrepareBulkRequestReference.set(delayedPrepareBulkRequest.rethrottle(newRequestsPerSecond));
        }
    }

    class DelayedPrepareBulkRequest {
        private final ThreadPool threadPool;
        private final Runnable command;
        private final float requestsPerSecond;
        private final Scheduler.ScheduledCancellable scheduled;

        DelayedPrepareBulkRequest(ThreadPool threadPool, float requestsPerSecond, TimeValue delay, Runnable command) {
            this.threadPool = threadPool;
            this.requestsPerSecond = requestsPerSecond;
            this.command = command;
            this.scheduled = threadPool.schedule(() -> {
                throttledNanos.addAndGet(delay.nanos());
                command.run();
            }, delay, ThreadPool.Names.GENERIC);
        }

        DelayedPrepareBulkRequest rethrottle(float newRequestsPerSecond) {
            if (newRequestsPerSecond < requestsPerSecond) {
                /* The user is attempting to slow the request down. We'll let the
                 * change in throttle take effect the next time we delay
                 * prepareBulkRequest. We can't just reschedule the request further
                 * out in the future because the bulk context might time out. */
                logger.debug(
                    "[{}]: skipping rescheduling because the new throttle [{}] is slower than the old one [{}]",
                    task.getId(),
                    newRequestsPerSecond,
                    requestsPerSecond
                );
                return this;
            }

            long remainingDelay = scheduled.getDelay(TimeUnit.NANOSECONDS);
            // Actually reschedule the task
            if (scheduled == null || false == scheduled.cancel()) {
                // Couldn't cancel, probably because the task has finished or been scheduled. Either way we have nothing to do here.
                logger.debug("[{}]: skipping rescheduling because we couldn't cancel the task", task.getId());
                return this;
            }

            /* Strangely enough getting here doesn't mean that you actually
             * cancelled the request, just that you probably did. If you stress
             * test it you'll find that requests sneak through. So each request
             * is given a runOnce boolean to prevent that. */
            TimeValue newDelay = newDelay(remainingDelay, newRequestsPerSecond);
            logger.debug("[{}]: rescheduling for [{}] in the future", task.getId(), newDelay);
            return new DelayedPrepareBulkRequest(threadPool, requestsPerSecond, newDelay, command);
        }

        /**
         * Scale back remaining delay to fit the new delay.
         */
        TimeValue newDelay(long remainingDelay, float newRequestsPerSecond) {
            if (remainingDelay < 0) {
                return timeValueNanos(0);
            }
            return timeValueNanos(round(remainingDelay * requestsPerSecond / newRequestsPerSecond));
        }
    }
}
