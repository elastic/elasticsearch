/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.timeValueNanos;

/**
 * Task storing information about a currently running BulkByScroll request.
 *
 * When the request is not sliced, this task is the only task created, and starts an action to perform search requests.
 *
 * When the request is sliced, this task can either represent a coordinating task (using
 * {@link BulkByScrollTask#setWorkerCount(int)}) or a worker task that performs search queries (using
 * {@link BulkByScrollTask#setWorker(float, Integer)}).
 *
 * We don't always know if this task will be a leader or worker task when it's created, because if slices is set to "auto" it may
 * be either depending on the number of shards in the source indices. We figure that out when the request is handled and set it on this
 * class with {@link #setWorkerCount(int)} or {@link #setWorker(float, Integer)}.
 */
public class BulkByScrollTask extends CancellableTask {

    private volatile LeaderBulkByScrollTaskState leaderState;
    private volatile WorkerBulkByScrollTaskState workerState;

    public BulkByScrollTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public BulkByScrollTask.Status getStatus() {
        if (isLeader()) {
            return leaderState.getStatus();
        }

        if (isWorker()) {
            return workerState.getStatus();
        }

        return emptyStatus();
    }

    /**
     * Build the status for this task given a snapshot of the information of running slices. This is only supported if the task is
     * set as a leader for slice subtasks
     */
    public TaskInfo taskInfoGivenSubtaskInfo(String localNodeId, List<TaskInfo> sliceInfo) {
        if (isLeader() == false) {
            throw new IllegalStateException("This task is not set to be a leader of other slice subtasks");
        }

        List<BulkByScrollTask.StatusOrException> sliceStatuses = Arrays.asList(
            new BulkByScrollTask.StatusOrException[leaderState.getSlices()]
        );
        for (TaskInfo t : sliceInfo) {
            BulkByScrollTask.Status status = (BulkByScrollTask.Status) t.status();
            sliceStatuses.set(status.getSliceId(), new BulkByScrollTask.StatusOrException(status));
        }
        Status status = leaderState.getStatus(sliceStatuses);
        return taskInfo(localNodeId, getDescription(), status);
    }

    private BulkByScrollTask.Status emptyStatus() {
        return new Status(Collections.emptyList(), getReasonCancelled());
    }

    /**
     * Returns true if this task is a leader for other slice subtasks
     */
    public boolean isLeader() {
        return leaderState != null;
    }

    /**
     * Sets this task to be a leader task for {@code slices} sliced subtasks
     */
    public void setWorkerCount(int slices) {
        if (isLeader()) {
            throw new IllegalStateException("This task is already a leader for other slice subtasks");
        }
        if (isWorker()) {
            throw new IllegalStateException("This task is already a worker");
        }

        leaderState = new LeaderBulkByScrollTaskState(this, slices);
    }

    /**
     * Returns the object that tracks the state of sliced subtasks. Throws IllegalStateException if this task is not set to be
     * a leader task.
     */
    public LeaderBulkByScrollTaskState getLeaderState() {
        if (isLeader() == false) {
            throw new IllegalStateException("This task is not set to be a leader for other slice subtasks");
        }
        return leaderState;
    }

    /**
     * Returns true if this task is a worker task that performs search requests. False otherwise
     */
    public boolean isWorker() {
        return workerState != null;
    }

    /**
     * Sets this task to be a worker task that performs search requests
     * @param requestsPerSecond How many search requests per second this task should make
     * @param sliceId If this is is a sliced task, which slice number this task corresponds to. Null if not sliced.
     */
    public void setWorker(float requestsPerSecond, @Nullable Integer sliceId) {
        if (isWorker()) {
            throw new IllegalStateException("This task is already a worker");
        }
        if (isLeader()) {
            throw new IllegalStateException("This task is already a leader for other slice subtasks");
        }

        workerState = new WorkerBulkByScrollTaskState(this, sliceId, requestsPerSecond);
        if (isCancelled()) {
            workerState.handleCancel();
        }
    }

    /**
     * Returns the object that manages sending search requests. Throws IllegalStateException if this task is not set to be a
     * worker task.
     */
    public WorkerBulkByScrollTaskState getWorkerState() {
        if (isWorker() == false) {
            throw new IllegalStateException("This task is not set to be a worker");
        }
        return workerState;
    }

    @Override
    public void onCancelled() {
        /*
         * If this task is a leader, we don't need to do anything extra because the cancel action cancels child tasks for us
         * If it's is a worker, we know how to cancel it here
         * If we don't know whether it's a leader or worker yet, we do nothing here. If the task is later set to be a worker, we cancel the
         * worker at that time.
         */
        if (isWorker()) {
            workerState.handleCancel();
        }
    }

    /**
     * This class acts as a builder for {@link Status}. Once the {@link Status} object is built by calling
     * {@link #buildStatus()} it is immutable. Used by an instance of {@link ObjectParser} when parsing from
     * XContent.
     */
    public static class StatusBuilder {
        private Integer sliceId = null;
        private Long total = null;
        private long updated = 0; // Not present during deleteByQuery
        private long created = 0; // Not present during updateByQuery
        private Long deleted = null;
        private Integer batches = null;
        private Long versionConflicts = null;
        private Long noops = null;
        private Long bulkRetries = null;
        private Long searchRetries = null;
        private TimeValue throttled = null;
        private Float requestsPerSecond = null;
        private String reasonCancelled = null;
        private TimeValue throttledUntil = null;
        private List<StatusOrException> sliceStatuses = new ArrayList<>();

        public void setSliceId(Integer sliceId) {
            this.sliceId = sliceId;
        }

        public void setTotal(Long total) {
            this.total = total;
        }

        public void setUpdated(Long updated) {
            this.updated = updated;
        }

        public void setCreated(Long created) {
            this.created = created;
        }

        public void setDeleted(Long deleted) {
            this.deleted = deleted;
        }

        public void setBatches(Integer batches) {
            this.batches = batches;
        }

        public void setVersionConflicts(Long versionConflicts) {
            this.versionConflicts = versionConflicts;
        }

        public void setNoops(Long noops) {
            this.noops = noops;
        }

        public void setRetries(Tuple<Long, Long> retries) {
            if (retries != null) {
                setBulkRetries(retries.v1());
                setSearchRetries(retries.v2());
            }
        }

        public void setBulkRetries(Long bulkRetries) {
            this.bulkRetries = bulkRetries;
        }

        public void setSearchRetries(Long searchRetries) {
            this.searchRetries = searchRetries;
        }

        public void setThrottled(Long throttled) {
            if (throttled != null) {
                this.throttled = new TimeValue(throttled, TimeUnit.MILLISECONDS);
            }
        }

        public void setRequestsPerSecond(Float requestsPerSecond) {
            if (requestsPerSecond != null) {
                requestsPerSecond = requestsPerSecond == -1 ? Float.POSITIVE_INFINITY : requestsPerSecond;
                this.requestsPerSecond = requestsPerSecond;
            }
        }

        public void setReasonCancelled(String reasonCancelled) {
            this.reasonCancelled = reasonCancelled;
        }

        public void setThrottledUntil(Long throttledUntil) {
            if (throttledUntil != null) {
                this.throttledUntil = new TimeValue(throttledUntil, TimeUnit.MILLISECONDS);
            }
        }

        public void setSliceStatuses(List<StatusOrException> sliceStatuses) {
            if (sliceStatuses != null) {
                this.sliceStatuses.addAll(sliceStatuses);
            }
        }

        public void addToSliceStatuses(StatusOrException statusOrException) {
            this.sliceStatuses.add(statusOrException);
        }

        public Status buildStatus() {
            if (sliceStatuses.isEmpty()) {
                try {
                    return new Status(
                        sliceId,
                        total,
                        updated,
                        created,
                        deleted,
                        batches,
                        versionConflicts,
                        noops,
                        bulkRetries,
                        searchRetries,
                        throttled,
                        requestsPerSecond,
                        reasonCancelled,
                        throttledUntil
                    );
                } catch (NullPointerException npe) {
                    throw new IllegalArgumentException("a required field is null when building Status");
                }
            } else {
                return new Status(sliceStatuses, reasonCancelled);
            }
        }
    }

    /**
     * Status of the reindex, update by query, or delete by query. While in
     * general we allow {@linkplain Task.Status} implementations to make
     * backwards incompatible changes to their {@link Task.Status#toXContent}
     * implementations, this one has become defacto standardized because Kibana
     * parses it. As such, we should be very careful about removing things from
     * this.
     */
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

        public static final String SLICE_ID_FIELD = "slice_id";
        public static final String TOTAL_FIELD = "total";
        public static final String UPDATED_FIELD = "updated";
        public static final String CREATED_FIELD = "created";
        public static final String DELETED_FIELD = "deleted";
        public static final String BATCHES_FIELD = "batches";
        public static final String VERSION_CONFLICTS_FIELD = "version_conflicts";
        public static final String NOOPS_FIELD = "noops";
        public static final String RETRIES_FIELD = "retries";
        public static final String RETRIES_BULK_FIELD = "bulk";
        public static final String RETRIES_SEARCH_FIELD = "search";
        public static final String THROTTLED_RAW_FIELD = "throttled_millis";
        public static final String THROTTLED_HR_FIELD = "throttled";
        public static final String REQUESTS_PER_SEC_FIELD = "requests_per_second";
        public static final String CANCELED_FIELD = "canceled";
        public static final String THROTTLED_UNTIL_RAW_FIELD = "throttled_until_millis";
        public static final String THROTTLED_UNTIL_HR_FIELD = "throttled_until";
        public static final String SLICES_FIELD = "slices";

        public static final Set<String> FIELDS_SET = new HashSet<>();
        static {
            FIELDS_SET.add(SLICE_ID_FIELD);
            FIELDS_SET.add(TOTAL_FIELD);
            FIELDS_SET.add(UPDATED_FIELD);
            FIELDS_SET.add(CREATED_FIELD);
            FIELDS_SET.add(DELETED_FIELD);
            FIELDS_SET.add(BATCHES_FIELD);
            FIELDS_SET.add(VERSION_CONFLICTS_FIELD);
            FIELDS_SET.add(NOOPS_FIELD);
            FIELDS_SET.add(RETRIES_FIELD);
            // No need for inner level fields for retries in the set of outer level fields
            FIELDS_SET.add(THROTTLED_RAW_FIELD);
            FIELDS_SET.add(THROTTLED_HR_FIELD);
            FIELDS_SET.add(REQUESTS_PER_SEC_FIELD);
            FIELDS_SET.add(CANCELED_FIELD);
            FIELDS_SET.add(THROTTLED_UNTIL_RAW_FIELD);
            FIELDS_SET.add(THROTTLED_UNTIL_HR_FIELD);
            FIELDS_SET.add(SLICES_FIELD);
        }

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

        public Status(
            Integer sliceId,
            long total,
            long updated,
            long created,
            long deleted,
            int batches,
            long versionConflicts,
            long noops,
            long bulkRetries,
            long searchRetries,
            TimeValue throttled,
            float requestsPerSecond,
            @Nullable String reasonCancelled,
            TimeValue throttledUntil
        ) {
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
            sliceId = in.readOptionalVInt();
            total = in.readVLong();
            updated = in.readVLong();
            created = in.readVLong();
            deleted = in.readVLong();
            batches = in.readVInt();
            versionConflicts = in.readVLong();
            noops = in.readVLong();
            bulkRetries = in.readVLong();
            searchRetries = in.readVLong();
            throttled = in.readTimeValue();
            requestsPerSecond = in.readFloat();
            reasonCancelled = in.readOptionalString();
            throttledUntil = in.readTimeValue();
            sliceStatuses = in.readCollectionAsList(stream -> stream.readOptionalWriteable(StatusOrException::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(sliceId);
            out.writeVLong(total);
            out.writeVLong(updated);
            out.writeVLong(created);
            out.writeVLong(deleted);
            out.writeVInt(batches);
            out.writeVLong(versionConflicts);
            out.writeVLong(noops);
            out.writeVLong(bulkRetries);
            out.writeVLong(searchRetries);
            out.writeTimeValue(throttled);
            out.writeFloat(requestsPerSecond);
            out.writeOptionalString(reasonCancelled);
            out.writeTimeValue(throttledUntil);
            out.writeCollection(sliceStatuses, StreamOutput::writeOptionalWriteable);
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

        public XContentBuilder innerXContent(XContentBuilder builder, Params params) throws IOException {
            if (sliceId != null) {
                builder.field(SLICE_ID_FIELD, sliceId);
            }
            builder.field(TOTAL_FIELD, total);
            if (params.paramAsBoolean(INCLUDE_UPDATED, true)) {
                builder.field(UPDATED_FIELD, updated);
            }
            if (params.paramAsBoolean(INCLUDE_CREATED, true)) {
                builder.field(CREATED_FIELD, created);
            }
            builder.field(DELETED_FIELD, deleted);
            builder.field(BATCHES_FIELD, batches);
            builder.field(VERSION_CONFLICTS_FIELD, versionConflicts);
            builder.field(NOOPS_FIELD, noops);
            builder.startObject(RETRIES_FIELD);
            {
                builder.field(RETRIES_BULK_FIELD, bulkRetries);
                builder.field(RETRIES_SEARCH_FIELD, searchRetries);
            }
            builder.endObject();
            builder.humanReadableField(THROTTLED_RAW_FIELD, THROTTLED_HR_FIELD, throttled);
            builder.field(REQUESTS_PER_SEC_FIELD, requestsPerSecond == Float.POSITIVE_INFINITY ? -1 : requestsPerSecond);
            if (reasonCancelled != null) {
                builder.field(CANCELED_FIELD, reasonCancelled);
            }
            builder.humanReadableField(THROTTLED_UNTIL_RAW_FIELD, THROTTLED_UNTIL_HR_FIELD, throttledUntil);
            if (false == sliceStatuses.isEmpty()) {
                builder.startArray(SLICES_FIELD);
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

        @Override
        public int hashCode() {
            return Objects.hash(
                sliceId,
                total,
                updated,
                created,
                deleted,
                batches,
                versionConflicts,
                noops,
                searchRetries,
                bulkRetries,
                throttled,
                requestsPerSecond,
                reasonCancelled,
                throttledUntil,
                sliceStatuses
            );
        }

        public boolean equalsWithoutSliceStatus(Object o, boolean includeUpdated, boolean includeCreated) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status other = (Status) o;
            return Objects.equals(sliceId, other.sliceId)
                && total == other.total
                && (includeUpdated == false || updated == other.updated)
                && (includeCreated == false || created == other.created)
                && deleted == other.deleted
                && batches == other.batches
                && versionConflicts == other.versionConflicts
                && noops == other.noops
                && searchRetries == other.searchRetries
                && bulkRetries == other.bulkRetries
                && Objects.equals(throttled, other.throttled)
                && requestsPerSecond == other.requestsPerSecond
                && Objects.equals(reasonCancelled, other.reasonCancelled)
                && Objects.equals(throttledUntil, other.throttledUntil);
        }

        @Override
        public boolean equals(Object o) {
            if (equalsWithoutSliceStatus(o, true, true)) {
                return Objects.equals(sliceStatuses, ((Status) o).sliceStatuses);
            } else {
                return false;
            }
        }

        private static int checkPositive(int value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be greater than 0 but was [" + value + "]");
            }
            return value;
        }

        private static long checkPositive(long value, String name) {
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
    public static class StatusOrException implements Writeable, ToXContentObject {
        private final Status status;
        private final Exception exception;

        public static final Set<String> EXPECTED_EXCEPTION_FIELDS = new HashSet<>();
        static {
            EXPECTED_EXCEPTION_FIELDS.add("type");
            EXPECTED_EXCEPTION_FIELDS.add("reason");
            EXPECTED_EXCEPTION_FIELDS.add("caused_by");
            EXPECTED_EXCEPTION_FIELDS.add("suppressed");
            EXPECTED_EXCEPTION_FIELDS.add("stack_trace");
            EXPECTED_EXCEPTION_FIELDS.add("header");
            EXPECTED_EXCEPTION_FIELDS.add("error");
            EXPECTED_EXCEPTION_FIELDS.add("root_cause");
        }

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
        public String toString() {
            if (exception != null) {
                return "BulkByScrollTask{error=" + Strings.toString(this) + "}";
            } else {
                return "BulkByScrollTask{status=" + Strings.toString(this) + "}";
            }
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
            return Objects.equals(status, other.status) && Objects.equals(exception, other.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(status, exception);
        }
    }

}
