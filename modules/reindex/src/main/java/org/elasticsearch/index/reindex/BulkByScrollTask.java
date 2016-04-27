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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
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
 * Task storing information about a currently running BulkByScroll request.
 */
public class BulkByScrollTask extends CancellableTask {
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
    private final AtomicLong retries = new AtomicLong(0);
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

    public BulkByScrollTask(long id, String type, String action, String description, TaskId parentTask, float requestsPerSecond) {
        super(id, type, action, description, parentTask);
        setRequestsPerSecond(requestsPerSecond);
    }

    @Override
    protected void onCancelled() {
        // Drop the throttle to 0, immediately rescheduling all outstanding tasks so the task will wake up and cancel itself.
        rethrottle(0);
    }

    @Override
    public Status getStatus() {
        return new Status(total.get(), updated.get(), created.get(), deleted.get(), batch.get(), versionConflicts.get(), noops.get(),
                retries.get(), timeValueNanos(throttledNanos.get()), getRequestsPerSecond(), getReasonCancelled(), throttledUntil());
    }

    private TimeValue throttledUntil() {
        DelayedPrepareBulkRequest delayed = delayedPrepareBulkRequestReference.get();
        if (delayed == null) {
            return timeValueNanos(0);
        }
        if (delayed.future == null) {
            return timeValueNanos(0);
        }
        return timeValueNanos(max(0, delayed.future.getDelay(TimeUnit.NANOSECONDS)));
    }

    /**
     * Total number of successfully processed documents.
     */
    public long getSuccessfullyProcessed() {
        return updated.get() + created.get() + deleted.get();
    }

    public static class Status implements Task.Status {
        public static final String NAME = "bulk-by-scroll";

        private final long total;
        private final long updated;
        private final long created;
        private final long deleted;
        private final int batches;
        private final long versionConflicts;
        private final long noops;
        private final long retries;
        private final TimeValue throttled;
        private final float requestsPerSecond;
        private final String reasonCancelled;
        private final TimeValue throttledUntil;

        public Status(long total, long updated, long created, long deleted, int batches, long versionConflicts, long noops, long retries,
                TimeValue throttled, float requestsPerSecond, @Nullable String reasonCancelled, TimeValue throttledUntil) {
            this.total = checkPositive(total, "total");
            this.updated = checkPositive(updated, "updated");
            this.created = checkPositive(created, "created");
            this.deleted = checkPositive(deleted, "deleted");
            this.batches = checkPositive(batches, "batches");
            this.versionConflicts = checkPositive(versionConflicts, "versionConflicts");
            this.noops = checkPositive(noops, "noops");
            this.retries = checkPositive(retries, "retries");
            this.throttled = throttled;
            this.requestsPerSecond = requestsPerSecond;
            this.reasonCancelled = reasonCancelled;
            this.throttledUntil = throttledUntil;
        }

        public Status(StreamInput in) throws IOException {
            total = in.readVLong();
            updated = in.readVLong();
            created = in.readVLong();
            deleted = in.readVLong();
            batches = in.readVInt();
            versionConflicts = in.readVLong();
            noops = in.readVLong();
            retries = in.readVLong();
            throttled = TimeValue.readTimeValue(in);
            requestsPerSecond = in.readFloat();
            reasonCancelled = in.readOptionalString();
            throttledUntil = TimeValue.readTimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeVLong(updated);
            out.writeVLong(created);
            out.writeVLong(deleted);
            out.writeVInt(batches);
            out.writeVLong(versionConflicts);
            out.writeVLong(noops);
            out.writeVLong(retries);
            throttled.writeTo(out);
            out.writeFloat(requestsPerSecond);
            out.writeOptionalString(reasonCancelled);
            throttledUntil.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerXContent(builder, params, true, true);
            return builder.endObject();
        }

        public XContentBuilder innerXContent(XContentBuilder builder, Params params, boolean includeCreated, boolean includeDeleted)
                throws IOException {
            builder.field("total", total);
            builder.field("updated", updated);
            if (includeCreated) {
                builder.field("created", created);
            }
            if (includeDeleted) {
                builder.field("deleted", deleted);
            }
            builder.field("batches", batches);
            builder.field("version_conflicts", versionConflicts);
            builder.field("noops", noops);
            builder.field("retries", retries);
            builder.timeValueField("throttled_millis", "throttled", throttled);
            builder.field("requests_per_second", requestsPerSecond == Float.POSITIVE_INFINITY ? "unlimited" : requestsPerSecond);
            if (reasonCancelled != null) {
                builder.field("canceled", reasonCancelled);
            }
            builder.timeValueField("throttled_until_millis", "throttled_until", throttledUntil);
            return builder;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("BulkIndexByScrollResponse[");
            innerToString(builder, true, true);
            return builder.append(']').toString();
        }

        public void innerToString(StringBuilder builder, boolean includeCreated, boolean includeDeleted) {
            builder.append("updated=").append(updated);
            if (includeCreated) {
                builder.append(",created=").append(created);
            }
            if (includeDeleted) {
                builder.append(",deleted=").append(deleted);
            }
            builder.append(",batches=").append(batches);
            builder.append(",versionConflicts=").append(versionConflicts);
            builder.append(",noops=").append(noops);
            builder.append(",retries=").append(retries);
            if (reasonCancelled != null) {
                builder.append(",canceled=").append(reasonCancelled);
            }
            builder.append(",throttledUntil=").append(throttledUntil);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        /**
         * The total number of documents this request will process. 0 means we don't yet know or, possibly, there are actually 0 documents
         * to process. Its ok that these have the same meaning because any request with 0 actual documents should be quite short lived.
         */
        public long getTotal() {
            return total;
        }

        /**
         * Count of documents updated.
         */
        public long getUpdated() {
            return updated;
        }

        /**
         * Count of documents created.
         */
        public long getCreated() {
            return created;
        }

        /**
         * Count of successful delete operations.
         */
        public long getDeleted() {
            return deleted;
        }

        /**
         * Number of scan responses this request has processed.
         */
        public int getBatches() {
            return batches;
        }

        /**
         * Number of version conflicts this request has hit.
         */
        public long getVersionConflicts() {
            return versionConflicts;
        }

        /**
         * Number of noops (skipped bulk items) as part of this request.
         */
        public long getNoops() {
            return noops;
        }

        /**
         * Number of retries that had to be attempted due to rejected executions.
         */
        public long getRetries() {
            return retries;
        }

        /**
         * The total time this request has throttled itself not including the current throttle time if it is currently sleeping.
         */
        public TimeValue getThrottled() {
            return throttled;
        }

        /**
         * The number of requests per second to which to throttle the request. 0 means unlimited.
         */
        public float getRequestsPerSecond() {
            return requestsPerSecond;
        }

        /**
         * The reason that the request was canceled or null if it hasn't been.
         */
        public String getReasonCancelled() {
            return reasonCancelled;
        }

        /**
         * Remaining delay of any current throttle sleep or 0 if not sleeping.
         */
        public TimeValue getThrottledUntil() {
            return throttledUntil;
        }

        private int checkPositive(int value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be greater than 0 but was [" + value + "]");
            }
            return value;
        }

        private long checkPositive(long value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be greater than 0 but was [" + value + "]");
            }
            return value;
        }
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

    void countCreated() {
        created.incrementAndGet();
    }

    void countUpdated() {
        updated.incrementAndGet();
    }

    void countDeleted() {
        deleted.incrementAndGet();
    }

    void countVersionConflict() {
        versionConflicts.incrementAndGet();
    }

    void countRetry() {
        retries.incrementAndGet();
    }

    float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    /**
     * Schedule prepareBulkRequestRunnable to run after some delay. This is where throttling plugs into reindexing so the request can be
     * rescheduled over and over again.
     */
    void delayPrepareBulkRequest(ThreadPool threadPool, TimeValue delay, AbstractRunnable prepareBulkRequestRunnable) {
        // Synchronize so we are less likely to schedule the same request twice.
        synchronized (delayedPrepareBulkRequestReference) {
            AbstractRunnable oneTime = new AbstractRunnable() {
                private final AtomicBoolean hasRun = new AtomicBoolean(false);

                @Override
                protected void doRun() throws Exception {
                    // Paranoia to prevent furiously rethrottling from running the command multiple times. Without this we totally can.
                    if (hasRun.compareAndSet(false, true)) {
                        prepareBulkRequestRunnable.run();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    prepareBulkRequestRunnable.onFailure(t);
                }
            };
            delayedPrepareBulkRequestReference.set(new DelayedPrepareBulkRequest(threadPool, getRequestsPerSecond(), delay, oneTime));
        }
    }

    private void setRequestsPerSecond(float requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
    }

    void rethrottle(float newRequestsPerSecond) {
        synchronized (delayedPrepareBulkRequestReference) {
            setRequestsPerSecond(newRequestsPerSecond);

            DelayedPrepareBulkRequest delayedPrepareBulkRequest = this.delayedPrepareBulkRequestReference.get();
            if (delayedPrepareBulkRequest == null) {
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
                public void onFailure(Throwable t) {
                    command.onFailure(t);
                }
            });
        }

        DelayedPrepareBulkRequest rethrottle(float newRequestsPerSecond) {
            if (newRequestsPerSecond != 0 && newRequestsPerSecond < requestsPerSecond) {
                /*
                 * The user is attempting to slow the request down. We'll let the change in throttle take effect the next time we delay
                 * prepareBulkRequest. We can't just reschedule the request further out in the future the bulk context might time out.
                 */
                return this;
            }

            long remainingDelay = future.getDelay(TimeUnit.NANOSECONDS);
            // Actually reschedule the task
            if (false == FutureUtils.cancel(future)) {
                // Couldn't cancel, probably because the task has finished or been scheduled. Either way we have nothing to do here.
                return this;
            }

            /*
             * Strangely enough getting here doesn't mean that you actually cancelled the request, just that you probably did. If you stress
             * test it you'll find that requests sneak through. So each request is given a runOnce boolean to prevent that.
             */
            TimeValue newDelay = newDelay(remainingDelay, newRequestsPerSecond);
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
}
