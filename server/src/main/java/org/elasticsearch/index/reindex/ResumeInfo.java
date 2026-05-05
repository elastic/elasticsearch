/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds resume state information for a {@link BulkByScrollTask} task to be resumed from a previous run. It may contain a WorkerResumeInfo
 * which keeps the state for a single worker task, or a map of SliceResumeInfo which keeps the state for each slice of a leader task.
 * It also has information about the original task that was relocated, so the user-facing taskID and start time are preserved in listings.
 * But the RelocationOrigin isn't accurate for sliced tasks, they have themselves as the origin, but for listing the leader is correct.
 * SourceTaskResult contains the result of the source task, and it is passed through relocation. The destination task persists this result
 * in .tasks to ensure the relocation chain is maintained even if the source task fails to store its result. This prevents the
 * destination task from becoming orphaned and unreachable through the relocation chain.
 * <p>
 * Note: For sliced tasks, resume info must include all slices, including those that are already completed. This ensures that the final
 * task has a complete result from all slices. A task may be resumed multiple times, so information for completed slices must be carried
 * forward to each subsequent resume until the task is fully completed. SourceTaskResult should not be present for sliced workers.
 * <p>
 * TODO: we can use List instead of Map for slices since the keys are required to be 0-based and contiguous.
 */
public record ResumeInfo(
    RelocationOrigin relocationOrigin,
    @Nullable WorkerResumeInfo worker,
    @Nullable Map<Integer, SliceStatus> slices,
    @Nullable TaskResult sourceTaskResult
) implements Writeable {

    public ResumeInfo(RelocationOrigin relocationOrigin, @Nullable WorkerResumeInfo worker, @Nullable Map<Integer, SliceStatus> slices) {
        this(relocationOrigin, worker, slices, null);
    }

    public ResumeInfo(
        RelocationOrigin relocationOrigin,
        @Nullable WorkerResumeInfo worker,
        @Nullable Map<Integer, SliceStatus> slices,
        @Nullable TaskResult sourceTaskResult
    ) {
        this.relocationOrigin = Objects.requireNonNull(relocationOrigin, "relocation origin cannot be null");
        if (worker == null && (slices == null || slices.size() < 2)) {
            throw new IllegalArgumentException("resume info requires a worker resume info or at minimum two slices");
        }
        if (worker != null && slices != null) {
            throw new IllegalArgumentException("resume info cannot contain both a worker resume info and slices resume info");
        }
        this.worker = worker;
        this.slices = slices != null ? Map.copyOf(slices) : null;
        this.sourceTaskResult = sourceTaskResult;
    }

    public ResumeInfo(StreamInput in) throws IOException {
        this(
            new RelocationOrigin(in), // if serialized, always present
            in.readOptionalNamedWriteable(WorkerResumeInfo.class),
            in.readOptionalImmutableMap(StreamInput::readVInt, SliceStatus::new),
            in.readOptionalWriteable(TaskResult::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(relocationOrigin);
        out.writeOptionalNamedWriteable(worker);
        out.writeOptionalMap(slices, StreamOutput::writeVInt, (o, v) -> v.writeTo(o));
        out.writeOptionalWriteable(sourceTaskResult);
    }

    public int getTotalSlices() {
        return slices == null ? 1 : slices.size();
    }

    public Optional<WorkerResumeInfo> getWorker() {
        return Optional.ofNullable(worker);
    }

    public Optional<SliceStatus> getSlice(int sliceId) {
        return slices == null ? Optional.empty() : Optional.ofNullable(slices.get(sliceId));
    }

    public boolean isSliceCompleted(int sliceId) {
        Optional<SliceStatus> slice = getSlice(sliceId);
        if (slice.isEmpty()) {
            throw new IllegalArgumentException("slice id " + sliceId + " does not exist in resume info");
        }
        return slice.get().isCompleted();
    }

    /**
     * Resume information for a single worker of a BulkByScrollTask.
     */
    public interface WorkerResumeInfo extends NamedWriteable {
        long startTimeEpochMillis();

        BulkByScrollTask.Status status();
    }

    /**
     * Resume information for a scroll-based BulkByScrollTask worker.
     */
    public record ScrollWorkerResumeInfo(
        String scrollId,
        long startTimeEpochMillis,
        BulkByScrollTask.Status status,
        @Nullable Version remoteVersion
    ) implements WorkerResumeInfo {

        public static final String NAME = "ScrollWorkerResumeInfo";

        public ScrollWorkerResumeInfo {
            Objects.requireNonNull(scrollId, "scrollId cannot be null");
            Objects.requireNonNull(status, "status cannot be null");
        }

        public ScrollWorkerResumeInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readLong(), new BulkByScrollTask.Status(in), in.readOptional(Version::readVersion));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(scrollId);
            out.writeLong(startTimeEpochMillis);
            status.writeTo(out);
            out.writeOptional((output, version) -> Version.writeVersion(version, output), remoteVersion);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

    }

    /**
     * Resume information for a PIT-based BulkByScrollTask worker.
     */
    public record PitWorkerResumeInfo(
        BytesReference pitId,
        Object[] searchAfterValues,
        long startTimeEpochMillis,
        BulkByScrollTask.Status status,
        @Nullable Version remoteVersion
    ) implements WorkerResumeInfo {
        public static final String NAME = "PitWorkerResumeInfo";

        public PitWorkerResumeInfo {
            Objects.requireNonNull(pitId, "pitId cannot be null");
            Objects.requireNonNull(searchAfterValues, "searchAfterValues cannot be null");
            Objects.requireNonNull(status, "status cannot be null");
        }

        public PitWorkerResumeInfo(StreamInput in) throws IOException {
            this(
                in.readBytesReference(),
                in.readArray(StreamInput::readGenericValue, Object[]::new),
                in.readLong(),
                new BulkByScrollTask.Status(in),
                in.readOptional(Version::readVersion)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(pitId);
            out.writeArray(StreamOutput::writeGenericValue, searchAfterValues);
            out.writeLong(startTimeEpochMillis);
            status.writeTo(out);
            out.writeOptional((output, version) -> Version.writeVersion(version, output), remoteVersion);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

    }

    /*
     * Holds the result of a worker task, either with a successful response or a failure exception */
    public record WorkerResult(@Nullable BulkByScrollResponse response, @Nullable Exception failure) implements Writeable {

        public WorkerResult {
            if (response != null && failure != null) {
                throw new IllegalArgumentException("worker result cannot contain both a response and failure");
            }
            if (response == null && failure == null) {
                throw new IllegalArgumentException("worker result requires a response or failure");
            }
        }

        public WorkerResult(StreamInput in) throws IOException {
            this(in.readOptional(BulkByScrollResponse::new), in.readOptionalException());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(response);
            out.writeOptionalException(failure);
        }

        public Optional<BulkByScrollResponse> getResponse() {
            return Optional.ofNullable(response);
        }

        public Optional<Exception> getFailure() {
            return Optional.ofNullable(failure);
        }
    }

    /**
     * Information for a single slice of a BulkByScrollTask.
     * It contains either:
     * - the WorkerResumeInfo for the worker that processed the slice if the task was not completed
     * - or the final result of the slice if it has completed (successfully or erroneously).
     */
    public record SliceStatus(int sliceId, @Nullable WorkerResumeInfo resumeInfo, @Nullable WorkerResult result) implements Writeable {
        public SliceStatus {
            if (resumeInfo != null && result != null) {
                throw new IllegalArgumentException("slice status cannot contain both resume info and result");
            }
            if (resumeInfo == null && result == null) {
                throw new IllegalArgumentException("slice status requires resume info or result");
            }
        }

        public SliceStatus(StreamInput in) throws IOException {
            this(in.readVInt(), in.readOptionalNamedWriteable(WorkerResumeInfo.class), in.readOptionalWriteable(WorkerResult::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(sliceId);
            out.writeOptionalNamedWriteable(resumeInfo);
            out.writeOptionalWriteable(result);
        }

        public boolean isCompleted() {
            return result != null;
        }
    }

    /**
     * Identity of the original task that was relocated. Propagated through relocation chains
     * so the user-facing task ID and start time are preserved across any number of hops.
     */
    public record RelocationOrigin(TaskId originalTaskId, long originalStartTimeMillis) implements Writeable {

        public RelocationOrigin {
            Objects.requireNonNull(originalTaskId, "originalTaskId cannot be null");
        }

        public RelocationOrigin(StreamInput in) throws IOException {
            this(TaskId.readFromStream(in), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeWriteable(originalTaskId);
            out.writeLong(originalStartTimeMillis);
        }
    }
}
