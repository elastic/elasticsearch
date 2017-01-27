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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;

/**
 * Task storing information about a currently running BulkByScroll request.
 */
public abstract class BulkByScrollTask extends CancellableTask {
    public BulkByScrollTask(long id, String type, String action, String description, TaskId parentTaskId) {
        super(id, type, action, description, parentTaskId);
    }

    /**
     * The number of sub-slices that are still running. {@link WorkingBulkByScrollTask} will always have 0 and
     * {@link ParentBulkByScrollTask} will return the number of waiting tasks. Used to decide how to perform rethrottling.
     */
    public abstract int runningSliceSubTasks();

    /**
     * Apply the {@code newRequestsPerSecond}.
     */
    public abstract void rethrottle(float newRequestsPerSecond);

    /*
     * Overridden to force children to return compatible status.
     */
    public abstract BulkByScrollTask.Status getStatus();

    /**
     * Build the status for this task given a snapshot of the information of running slices.
     */
    public abstract TaskInfo getInfoGivenSliceInfo(String localNodeId, List<TaskInfo> sliceInfo);

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public static class Status implements Task.Status, SuccessfullyProcessed {
        public static final String NAME = "bulk-by-scroll";

        /**
         * XContent param name to indicate if "created" count must be included
         * in the response.
         */
        public static final String INCLUDE_CREATED = "include_created";

        /**
         * XContent param name to indicate if "updated" count must be included
         * in the response.
         */
        public static final String INCLUDE_UPDATED = "include_updated";

        private final Integer sliceId;
        private final long total;
        private final long updated;
        private final long created;
        private final long deleted;
        private final int batches;
        private final long versionConflicts;
        private final long noops;
        private final long bulkRetries;
        private final long searchRetries;
        private final TimeValue throttled;
        private final float requestsPerSecond;
        private final String reasonCancelled;
        private final TimeValue throttledUntil;
        private final List<StatusOrException> sliceStatuses;

        public Status(Integer sliceId, long total, long updated, long created, long deleted, int batches, long versionConflicts, long noops,
                long bulkRetries, long searchRetries, TimeValue throttled, float requestsPerSecond, @Nullable String reasonCancelled,
                TimeValue throttledUntil) {
            this.sliceId = sliceId == null ? null : checkPositive(sliceId, "sliceId");
            this.total = checkPositive(total, "total");
            this.updated = checkPositive(updated, "updated");
            this.created = checkPositive(created, "created");
            this.deleted = checkPositive(deleted, "deleted");
            this.batches = checkPositive(batches, "batches");
            this.versionConflicts = checkPositive(versionConflicts, "versionConflicts");
            this.noops = checkPositive(noops, "noops");
            this.bulkRetries = checkPositive(bulkRetries, "bulkRetries");
            this.searchRetries = checkPositive(searchRetries, "searchRetries");
            this.throttled = throttled;
            this.requestsPerSecond = requestsPerSecond;
            this.reasonCancelled = reasonCancelled;
            this.throttledUntil = throttledUntil;
            this.sliceStatuses = emptyList();
        }

        /**
         * Constructor merging many statuses.
         *
         * @param sliceStatuses Statuses of sub requests that this task was sliced into.
         * @param reasonCancelled Reason that this *this* task was cancelled. Note that each entry in {@code sliceStatuses} can be cancelled
         *        independently of this task but if this task is cancelled then the workers *should* be cancelled.
         */
        public Status(List<StatusOrException> sliceStatuses, @Nullable String reasonCancelled) {
            sliceId = null;
            this.reasonCancelled = reasonCancelled;

            long mergedTotal = 0;
            long mergedUpdated = 0;
            long mergedCreated = 0;
            long mergedDeleted = 0;
            int mergedBatches = 0;
            long mergedVersionConflicts = 0;
            long mergedNoops = 0;
            long mergedBulkRetries = 0;
            long mergedSearchRetries = 0;
            long mergedThrottled = 0;
            float mergedRequestsPerSecond = 0;
            long mergedThrottledUntil = Long.MAX_VALUE;

            for (StatusOrException slice : sliceStatuses) {
                if (slice == null) {
                    // Hasn't returned yet.
                    continue;
                }
                if (slice.status == null) {
                    // This slice failed catastrophically so it doesn't count towards the status
                    continue;
                }
                mergedTotal += slice.status.getTotal();
                mergedUpdated += slice.status.getUpdated();
                mergedCreated += slice.status.getCreated();
                mergedDeleted += slice.status.getDeleted();
                mergedBatches += slice.status.getBatches();
                mergedVersionConflicts += slice.status.getVersionConflicts();
                mergedNoops += slice.status.getNoops();
                mergedBulkRetries += slice.status.getBulkRetries();
                mergedSearchRetries += slice.status.getSearchRetries();
                mergedThrottled += slice.status.getThrottled().nanos();
                mergedRequestsPerSecond += slice.status.getRequestsPerSecond();
                mergedThrottledUntil = min(mergedThrottledUntil, slice.status.getThrottledUntil().nanos());
            }

            total = mergedTotal;
            updated = mergedUpdated;
            created = mergedCreated;
            deleted = mergedDeleted;
            batches = mergedBatches;
            versionConflicts = mergedVersionConflicts;
            noops = mergedNoops;
            bulkRetries = mergedBulkRetries;
            searchRetries = mergedSearchRetries;
            throttled = timeValueNanos(mergedThrottled);
            requestsPerSecond = mergedRequestsPerSecond;
            throttledUntil = timeValueNanos(mergedThrottledUntil == Long.MAX_VALUE ? 0 : mergedThrottledUntil);
            this.sliceStatuses = sliceStatuses;
        }

        public Status(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
                sliceId = in.readOptionalVInt();
            } else {
                sliceId = null;
            }
            total = in.readVLong();
            updated = in.readVLong();
            created = in.readVLong();
            deleted = in.readVLong();
            batches = in.readVInt();
            versionConflicts = in.readVLong();
            noops = in.readVLong();
            bulkRetries = in.readVLong();
            searchRetries = in.readVLong();
            throttled = new TimeValue(in);
            requestsPerSecond = in.readFloat();
            reasonCancelled = in.readOptionalString();
            throttledUntil = new TimeValue(in);
            if (in.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
                sliceStatuses = in.readList(stream -> stream.readOptionalWriteable(StatusOrException::new));
            } else {
                sliceStatuses = emptyList();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
                out.writeOptionalVInt(sliceId);
            }
            out.writeVLong(total);
            out.writeVLong(updated);
            out.writeVLong(created);
            out.writeVLong(deleted);
            out.writeVInt(batches);
            out.writeVLong(versionConflicts);
            out.writeVLong(noops);
            out.writeVLong(bulkRetries);
            out.writeVLong(searchRetries);
            throttled.writeTo(out);
            out.writeFloat(requestsPerSecond);
            out.writeOptionalString(reasonCancelled);
            throttledUntil.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
                out.writeVInt(sliceStatuses.size());
                for (StatusOrException sliceStatus : sliceStatuses) {
                    out.writeOptionalWriteable(sliceStatus);
                }
            }
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerXContent(builder, params);
            return builder.endObject();
        }

        public XContentBuilder innerXContent(XContentBuilder builder, Params params)
                throws IOException {
            if (sliceId != null) {
                builder.field("slice_id", sliceId);
            }
            builder.field("total", total);
            if (params.paramAsBoolean(INCLUDE_UPDATED, true)) {
                builder.field("updated", updated);
            }
            if (params.paramAsBoolean(INCLUDE_CREATED, true)) {
                builder.field("created", created);
            }
            builder.field("deleted", deleted);
            builder.field("batches", batches);
            builder.field("version_conflicts", versionConflicts);
            builder.field("noops", noops);
            builder.startObject("retries"); {
                builder.field("bulk", bulkRetries);
                builder.field("search", searchRetries);
            }
            builder.endObject();
            builder.timeValueField("throttled_millis", "throttled", throttled);
            builder.field("requests_per_second", requestsPerSecond == Float.POSITIVE_INFINITY ? -1 : requestsPerSecond);
            if (reasonCancelled != null) {
                builder.field("canceled", reasonCancelled);
            }
            builder.timeValueField("throttled_until_millis", "throttled_until", throttledUntil);
            if (false == sliceStatuses.isEmpty()) {
                builder.startArray("slices");
                for (StatusOrException slice : sliceStatuses) {
                    if (slice == null) {
                        builder.nullValue();
                    } else {
                        slice.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            return builder;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("BulkIndexByScrollResponse[");
            innerToString(builder);
            return builder.append(']').toString();
        }

        public void innerToString(StringBuilder builder) {
            builder.append("sliceId=").append(sliceId);
            builder.append(",updated=").append(updated);
            builder.append(",created=").append(created);
            builder.append(",deleted=").append(deleted);
            builder.append(",batches=").append(batches);
            builder.append(",versionConflicts=").append(versionConflicts);
            builder.append(",noops=").append(noops);
            builder.append(",retries=").append(bulkRetries);
            if (reasonCancelled != null) {
                builder.append(",canceled=").append(reasonCancelled);
            }
            builder.append(",throttledUntil=").append(throttledUntil);
            if (false == sliceStatuses.isEmpty()) {
                builder.append(",workers=").append(sliceStatuses);
            }
        }

        /**
         * The id of the slice that this status is reporting or {@code null} if this isn't the status of a sub-slice.
         */
        Integer getSliceId() {
            return sliceId;
        }

        /**
         * The total number of documents this request will process. 0 means we don't yet know or, possibly, there are actually 0 documents
         * to process. Its ok that these have the same meaning because any request with 0 actual documents should be quite short lived.
         */
        public long getTotal() {
            return total;
        }

        @Override
        public long getUpdated() {
            return updated;
        }

        @Override
        public long getCreated() {
            return created;
        }

        @Override
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
         * Number of retries that had to be attempted due to bulk actions being rejected.
         */
        public long getBulkRetries() {
            return bulkRetries;
        }

        /**
         * Number of retries that had to be attempted due to search actions being rejected.
         */
        public long getSearchRetries() {
            return searchRetries;
        }

        /**
         * The total time this request has throttled itself not including the current throttle time if it is currently sleeping.
         */
        public TimeValue getThrottled() {
            return throttled;
        }

        /**
         * The number of requests per second to which to throttle the request. Float.POSITIVE_INFINITY means unlimited.
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

        /**
         * Statuses of the sub requests into which this sub-request was sliced. Empty if this request wasn't sliced into sub-requests.
         */
        public List<StatusOrException> getSliceStatuses() {
            return sliceStatuses;
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

    /**
     * The status of a slice of the request. Successful requests store the {@link StatusOrException#status} while failing requests store a
     * {@link StatusOrException#exception}.
     */
    public static class StatusOrException implements Writeable, ToXContent {
        private final Status status;
        private final Exception exception;

        public StatusOrException(Status status) {
            this.status = status;
            exception = null;
        }

        public StatusOrException(Exception exception) {
            status = null;
            this.exception = exception;
        }

        /**
         * Read from a stream.
         */
        public StatusOrException(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                status = new Status(in);
                exception = null;
            } else {
                status = null;
                exception = in.readException();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (exception == null) {
                out.writeBoolean(true);
                status.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeException(exception);
            }
        }

        public Status getStatus() {
            return status;
        }

        public Exception getException() {
            return exception;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (exception == null) {
                status.toXContent(builder, params);
            } else {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, exception);
                builder.endObject();
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != BulkByScrollTask.StatusOrException.class) {
                return false;
            }
            BulkByScrollTask.StatusOrException other = (StatusOrException) obj;
            return Objects.equals(status, other.status)
                    && Objects.equals(exception, other.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(status, exception);
        }
    }

}
