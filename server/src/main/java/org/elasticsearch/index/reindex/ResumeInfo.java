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
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds resume state information for a {@link BulkByScrollTask} task to be resumed from a previous run. It may contain a WorkerResumeInfo
 * which keeps the state for a single worker task, or a map of SliceResumeInfo which keeps the state for each slice of a leader task.
 */
public record ResumeInfo(@Nullable WorkerResumeInfo worker, @Nullable Map<Integer, SliceStatus> slices) implements Writeable {

    public ResumeInfo(@Nullable WorkerResumeInfo worker, @Nullable Map<Integer, SliceStatus> slices) {
        if (worker == null && (slices == null || slices.size() < 2)) {
            throw new IllegalArgumentException("resume info requires a worker resume info or at minimum two slices");

        }
        if (worker != null && slices != null) {
            throw new IllegalArgumentException("resume info cannot contain both a worker resume info and slices resume info");
        }
        this.worker = worker;
        this.slices = slices != null ? Map.copyOf(slices) : null;
    }

    public ResumeInfo(StreamInput in) throws IOException {
        this(in.readOptionalNamedWriteable(WorkerResumeInfo.class), in.readOptionalImmutableMap(StreamInput::readVInt, SliceStatus::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(worker);
        out.writeOptionalMap(slices, StreamOutput::writeVInt, (o, v) -> v.writeTo(o));
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

    /**
     * Resume information for a single worker of a BulkByScrollTask.
     */
    public interface WorkerResumeInfo extends NamedWriteable {
        long startTime();

        BulkByScrollTask.Status status();
    }

    /**
     * Resume information for a scroll-based BulkByScrollTask worker.
     */
    public record ScrollWorkerResumeInfo(String scrollId, long startTime, BulkByScrollTask.Status status, @Nullable Version remoteVersion)
        implements
            WorkerResumeInfo {
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
            out.writeLong(startTime);
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
    }

}
