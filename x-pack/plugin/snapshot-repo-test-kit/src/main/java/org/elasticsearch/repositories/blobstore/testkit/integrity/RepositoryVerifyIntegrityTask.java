/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class RepositoryVerifyIntegrityTask extends CancellableTask {

    private volatile Supplier<Status> statusSupplier;

    public RepositoryVerifyIntegrityTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTaskId, headers);
    }

    public void setStatusSupplier(Supplier<Status> statusSupplier) {
        this.statusSupplier = statusSupplier;
    }

    @Override
    public Status getStatus() {
        return Optional.ofNullable(statusSupplier).map(Supplier::get).orElse(null);
    }

    public record Status(
        String repositoryName,
        long repositoryGeneration,
        String repositoryUUID,
        long snapshotCount,
        long snapshotsVerified,
        long indexCount,
        long indicesVerified,
        long indexSnapshotCount,
        long indexSnapshotsVerified,
        long blobsVerified,
        long blobBytesVerified,
        long throttledNanos
    ) implements org.elasticsearch.tasks.Task.Status {

        public static final String NAME = "verify_repository_integrity_status";

        public Status(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readVLong(),
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repositoryName);
            out.writeVLong(repositoryGeneration);
            out.writeString(repositoryUUID);
            out.writeVLong(snapshotCount);
            out.writeVLong(snapshotsVerified);
            out.writeVLong(indexCount);
            out.writeVLong(indicesVerified);
            out.writeVLong(indexSnapshotCount);
            out.writeVLong(indexSnapshotsVerified);
            out.writeVLong(blobsVerified);
            out.writeVLong(blobBytesVerified);
            out.writeVLong(throttledNanos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("repository");
            builder.field("name", repositoryName);
            builder.field("uuid", repositoryUUID);
            builder.field("generation", repositoryGeneration);
            builder.endObject();
            builder.startObject("snapshots");
            builder.field("verified", snapshotsVerified);
            builder.field("total", snapshotCount);
            builder.endObject();
            builder.startObject("indices");
            builder.field("verified", indicesVerified);
            builder.field("total", indexCount);
            builder.endObject();
            builder.startObject("index_snapshots");
            builder.field("verified", indexSnapshotsVerified);
            builder.field("total", indexSnapshotCount);
            builder.endObject();
            builder.startObject("blobs");
            builder.field("verified", blobsVerified);
            if (throttledNanos > 0) {
                builder.humanReadableField("verified_size_in_bytes", "verified_size", ByteSizeValue.ofBytes(blobBytesVerified));
                builder.humanReadableField("throttled_time_in_millis", "throttled_time", TimeValue.timeValueNanos(throttledNanos));
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
