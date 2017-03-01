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

package org.elasticsearch.action.bulk.byscroll;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static java.lang.Math.round;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;

/**
 * {@link BulkByScrollTask} subclass for tasks that actually perform the work. Compare to {@link ParentBulkByScrollTask}.
 */
public class WorkingBulkByScrollTask extends BulkByScrollTask implements SuccessfullyProcessed {
    private static final Logger logger = ESLoggerFactory.getLogger(BulkByScrollTask.class.getPackage().getName());

    /**
     * The id of the slice that this task is processing or {@code null} if this task isn't for a sliced request.
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

    public WorkingBulkByScrollTask(long id, String type, String action, String description, TaskId parentTask, Integer sliceId,
            float requestsPerSecond) {
        super(id, type, action, description, parentTask);
        this.sliceId = sliceId;
        setRequestsPerSecond(requestsPerSecond);
    }

    @Override
    public Status getStatus() {
        return new Status(sliceId, total.get(), updated.get(), created.get(), deleted.get(), batch.get(), versionConflicts.get(),
                noops.get(), bulkRetries.get(), searchRetries.get(), timeValueNanos(throttledNanos.get()), getRequestsPerSecond(),
                getReasonCancelled(), throttledUntil());
    }

    @Override
    protected void onCancelled() {
        // Drop the throttle to 0, immediately rescheduling all outstanding tasks so the task will wake up and cancel itself.
        rethrottle(0);
    }

    @Override
    public int runningSliceSubTasks() {
        return 0;
    }

    @Override
    public TaskInfo getInfoGivenSliceInfo(String localNodeId, List<TaskInfo> sliceInfo) {
        throw new UnsupportedOperationException("This is only supported by " + ParentBulkByScrollTask.class.getName() + ".");
    }

    TimeValue throttledUntil() {
        DelayedPrepareBulkRequest delayed = delayedPrepareBulkRequestReference.get();
        if (delayed == null) {
            return timeValueNanos(0);
        }
        if (delayed.future == null) {
            return timeValueNanos(0);
        }
        return timeValueNanos(max(0, delayed.future.getDelay(TimeUnit.NANOSECONDS)));
    }

    void setTotal(long totalHits) {
        total.set(totalHits);
    }

    void countBatch() {
        batch.incrementAndGet();
    }

    void countNoop() {
        noops.incrementAndGet();
    }

    @Override
    public long getCreated() {
        return created.get();
    }

    void countCreated() {
        created.incrementAndGet();
    }

    @Override
    public long getUpdated() {
        return updated.get();
    }

    void countUpdated() {
        updated.incrementAndGet();
    }

    @Override
    public long getDeleted() {
        return deleted.get();
    }

    void countDeleted() {
        deleted.incrementAndGet();
    }

    void countVersionConflict() {
        versionConflicts.incrementAndGet();
    }

    void countBulkRetry() {
        bulkRetries.incrementAndGet();
    }

    public void countSearchRetry() {
        searchRetries.incrementAndGet();
    }

    float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    /**
     * Schedule prepareBulkRequestRunnable to run after some delay. This is where throttling plugs into reindexing so the request can be
     * rescheduled over and over again.
     */
    void delayPrepareBulkRequest(ThreadPool threadPool, TimeValue lastBatchStartTime, int lastBatchSize,
            AbstractRunnable prepareBulkRequestRunnable) {
        // Synchronize so we are less likely to schedule the same request twice.
        synchronized (delayedPrepareBulkRequestReference) {
            TimeValue delay = throttleWaitTime(lastBatchStartTime, lastBatchSize);
            delayedPrepareBulkRequestReference.set(new DelayedPrepareBulkRequest(threadPool, getRequestsPerSecond(),
                    delay, new RunOnce(prepareBulkRequestRunnable)));
        }
    }

    TimeValue throttleWaitTime(TimeValue lastBatchStartTime, int lastBatchSize) {
        long earliestNextBatchStartTime = lastBatchStartTime.nanos() + (long) perfectlyThrottledBatchTime(lastBatchSize);
        return timeValueNanos(max(0, earliestNextBatchStartTime - System.nanoTime()));
    }

    /**
     * How many nanoseconds should a batch of lastBatchSize have taken if it were perfectly throttled? Package private for testing.
     */
    float perfectlyThrottledBatchTime(int lastBatchSize) {
        if (requestsPerSecond == Float.POSITIVE_INFINITY) {
            return 0;
        }
        //       requests
        // ------------------- == seconds
        // request per seconds
        float targetBatchTimeInSeconds = lastBatchSize / requestsPerSecond;
        // nanoseconds per seconds * seconds == nanoseconds
        return TimeUnit.SECONDS.toNanos(1) * targetBatchTimeInSeconds;
    }

    private void setRequestsPerSecond(float requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
    }

    @Override
    public void rethrottle(float newRequestsPerSecond) {
        synchronized (delayedPrepareBulkRequestReference) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}]: Rethrottling to [{}] requests per second", getId(), newRequestsPerSecond);
            }
            setRequestsPerSecond(newRequestsPerSecond);

            DelayedPrepareBulkRequest delayedPrepareBulkRequest = this.delayedPrepareBulkRequestReference.get();
            if (delayedPrepareBulkRequest == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}]: Skipping rescheduling because there is no scheduled task", getId());
                }
                // No request has been queued yet so nothing to reschedule.
                return;
            }

            this.delayedPrepareBulkRequestReference.set(delayedPrepareBulkRequest.rethrottle(newRequestsPerSecond));
        }
    }

    class DelayedPrepareBulkRequest {
        private final ThreadPool threadPool;
        private final AbstractRunnable command;
        private final float requestsPerSecond;
        private final ScheduledFuture<?> future;

        DelayedPrepareBulkRequest(ThreadPool threadPool, float requestsPerSecond, TimeValue delay, AbstractRunnable command) {
            this.threadPool = threadPool;
            this.requestsPerSecond = requestsPerSecond;
            this.command = command;
            this.future = threadPool.schedule(delay, ThreadPool.Names.GENERIC, new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    throttledNanos.addAndGet(delay.nanos());
                    command.run();
                }

                @Override
                public void onFailure(Exception e) {
                    command.onFailure(e);
                }
            });
        }

        DelayedPrepareBulkRequest rethrottle(float newRequestsPerSecond) {
            if (newRequestsPerSecond != 0 && newRequestsPerSecond < requestsPerSecond) {
                /*
                 * The user is attempting to slow the request down. We'll let the change in throttle take effect the next time we delay
                 * prepareBulkRequest. We can't just reschedule the request further out in the future the bulk context might time out.
                 */
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}]: Skipping rescheduling because the new throttle [{}] is slower than the old one [{}].", getId(),
                            newRequestsPerSecond, requestsPerSecond);
                }
                return this;
            }

            long remainingDelay = future.getDelay(TimeUnit.NANOSECONDS);
            // Actually reschedule the task
            if (false == FutureUtils.cancel(future)) {
                // Couldn't cancel, probably because the task has finished or been scheduled. Either way we have nothing to do here.
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}]: Skipping rescheduling we couldn't cancel the task.", getId());
                }
                return this;
            }

            /*
             * Strangely enough getting here doesn't mean that you actually cancelled the request, just that you probably did. If you stress
             * test it you'll find that requests sneak through. So each request is given a runOnce boolean to prevent that.
             */
            TimeValue newDelay = newDelay(remainingDelay, newRequestsPerSecond);
            if (logger.isDebugEnabled()) {
                logger.debug("[{}]: Rescheduling for [{}] in the future.", getId(), newDelay);
            }
            return new DelayedPrepareBulkRequest(threadPool, requestsPerSecond, newDelay, command);
        }

        /**
         * Scale back remaining delay to fit the new delay.
         */
        TimeValue newDelay(long remainingDelay, float newRequestsPerSecond) {
            if (remainingDelay < 0 || newRequestsPerSecond == 0) {
                return timeValueNanos(0);
            }
            return timeValueNanos(round(remainingDelay * requestsPerSecond / newRequestsPerSecond));
        }
    }

    /**
     * Runnable that can only be run one time. This is paranoia to prevent furiously rethrottling from running the command multiple times.
     * Without it the command would be run multiple times.
     */
    private static class RunOnce extends AbstractRunnable {
        private final AtomicBoolean hasRun = new AtomicBoolean(false);
        private final AbstractRunnable delegate;

        RunOnce(AbstractRunnable delegate) {
            this.delegate = delegate;
        }

        @Override
        protected void doRun() throws Exception {
            if (hasRun.compareAndSet(false, true)) {
                delegate.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }
    }
}
