/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A chunk of response to be streamed to the waiting client.
 *
 * @param type indicates the type of this chunk.
 * @param anomaly a textual description of the anomaly found, or {@code null} if this chunk does not describe an anomaly.
 * @param snapshotId the ID of the snapshot to which this chunk pertains, or {@code null} if this chunk does not pertain to a particular
 *                  snapshot.
 * @param snapshotInfo the raw {@link SnapshotInfo} for the snapshot, or {@code null}.
 * @param indexDescription information about the index to which this chunk pertains, or {@code null} if this chunk does not pertain to
 *                         a particular index.
 * @param shardId the ID of the shard to which this chunk pertains, or {@code -1} if this chunk does not pertain to a particular shard.
 * @param shardGeneration the {@link ShardGeneration} for the given shard, or {@code null} if not relevant.
 * @param blobName the name of the blob to which this chunk pertains, or {@code null} if this chunk does not pertain to a particular blob.
 * @param physicalFileName the name of the Lucene file to which this chunk pertains, or {@code null} if this chunk does not pertain to a
 *                         particular Lucene file.
 * @param partIndex the index of the part of the file represented by the blob to which this chunk pertains, or {@code -1} if this chunk does
 *                  not pertain to a particular part.
 * @param partCount the number of parts into which the file to which this chunk pertains is divided, or {@code -1} if not applicable.
 * @param fileLength the length of the Lucene file to which this chunk pertains, or {@link ByteSizeValue#MINUS_ONE} if not applicable.
 * @param partLength the length of the file part to which this chunk pertains, or {@link ByteSizeValue#MINUS_ONE} if not applicable.
 * @param blobLength the length of the blob to which this chunk pertains, or {@link ByteSizeValue#MINUS_ONE} if not applicable.
 * @param totalSnapshotCount the total number of snapshots which involve the index to which this chunk pertains, or {@code -1} if not
 *                           applicable.
 * @param restorableSnapshotCount the number of restorable snapshots which involve the index to which this chunk pertains, or {@code -1} if
 *                                not applicable.
 * @param exception an exception which relates to the failure described by this chunk, or {@code null} if not applicable.
 */
public record RepositoryVerifyIntegrityResponseChunk(
    long timestampMillis,
    Type type,
    @Nullable String anomaly,
    @Nullable SnapshotId snapshotId,
    @Nullable SnapshotInfo snapshotInfo,
    @Nullable IndexDescription indexDescription,
    int shardId,
    @Nullable ShardGeneration shardGeneration,
    @Nullable String blobName,
    @Nullable String physicalFileName,
    int partIndex,
    int partCount,
    ByteSizeValue fileLength,
    ByteSizeValue partLength,
    ByteSizeValue blobLength,
    int totalSnapshotCount,
    int restorableSnapshotCount,
    @Nullable Exception exception
) implements Writeable, ToXContentFragment {

    public enum Type {
        /**
         * The first chunk sent. Used to indicate that the verification has successfully started, and therefore we should start to send a
         * 200 OK response to the client.
         */
        START_RESPONSE,

        /**
         * This chunk contains the raw {@link SnapshotInfo} for a snapshot.
         */
        SNAPSHOT_INFO,

        /**
         * This chunk contains information about the restorability of an index.
         */
        INDEX_RESTORABILITY,

        /**
         * This chunk describes an anomaly found during verification.
         */
        ANOMALY,
    }

    public RepositoryVerifyIntegrityResponseChunk {
        if (fileLength == null
            || partLength == null
            || blobLength == null
            || shardId < -1
            || partIndex < -1
            || partCount < -1
            || totalSnapshotCount < -1
            || restorableSnapshotCount < -1
            || (totalSnapshotCount >= 0 != restorableSnapshotCount >= 0)) {
            throw new IllegalArgumentException("invalid: " + this);
        }
    }

    public RepositoryVerifyIntegrityResponseChunk(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            // TODO enum serialization tests
            in.readEnum(Type.class),
            in.readOptionalString(),
            in.readOptionalWriteable(SnapshotId::new),
            in.readOptionalWriteable(SnapshotInfo::readFrom),
            in.readOptionalWriteable(IndexDescription::new),
            in.readInt(),
            in.readOptionalWriteable(ShardGeneration::new),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readInt(),
            in.readInt(),
            ByteSizeValue.readFrom(in),
            ByteSizeValue.readFrom(in),
            ByteSizeValue.readFrom(in),
            in.readInt(),
            in.readInt(),
            in.readOptional(StreamInput::readException)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestampMillis);
        out.writeEnum(type);
        out.writeOptionalString(anomaly);
        out.writeOptionalWriteable(snapshotId);
        out.writeOptionalWriteable(snapshotInfo);
        out.writeOptionalWriteable(indexDescription);
        out.writeInt(shardId);
        out.writeOptionalWriteable(shardGeneration);
        out.writeOptionalString(blobName);
        out.writeOptionalString(physicalFileName);
        out.writeInt(partIndex);
        out.writeInt(partCount);
        fileLength.writeTo(out);
        partLength.writeTo(out);
        blobLength.writeTo(out);
        out.writeInt(totalSnapshotCount);
        out.writeInt(restorableSnapshotCount);
        out.writeOptional(StreamOutput::writeException, exception);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.timestampFieldsFromUnixEpochMillis("timestamp_in_millis", "timestamp", timestampMillis);

        if (anomaly() != null) {
            builder.field("anomaly", anomaly());
        }

        if (snapshotInfo() != null) {
            builder.field("snapshot");
            snapshotInfo().toXContentExternal(builder, params);
        } else if (snapshotId() != null) {
            builder.startObject("snapshot");
            builder.field("snapshot", snapshotId().getName());
            builder.field("uuid", snapshotId().getUUID());
            builder.endObject();
        }

        if (indexDescription() != null) {
            builder.field("index", indexDescription(), params);
        }
        if (shardId() >= 0) {
            builder.field("shard_id", shardId());
        }
        if (shardGeneration() != null) {
            builder.field("shard_generation", shardGeneration(), params);
        }
        if (blobName() != null) {
            builder.field("blob_name", blobName());
        }
        if (physicalFileName() != null) {
            builder.field("physical_file_name", physicalFileName());
        }
        if (partIndex() >= 0) {
            builder.field("part_index", partIndex());
        }
        if (partCount() >= 0) {
            builder.field("part_count", partCount());
        }
        if (fileLength() != ByteSizeValue.MINUS_ONE) {
            builder.humanReadableField("file_length_in_bytes", "file_length", fileLength());
        }
        if (partLength() != ByteSizeValue.MINUS_ONE) {
            builder.humanReadableField("part_length_in_bytes", "part_length", partLength());
        }
        if (blobLength() != ByteSizeValue.MINUS_ONE) {
            builder.humanReadableField("blob_length_in_bytes", "blob_length", blobLength());
        }
        if (totalSnapshotCount() >= 0 && restorableSnapshotCount() >= 0) {
            builder.startObject("snapshot_restorability");
            builder.field("total_snapshot_count", totalSnapshotCount());
            builder.field("restorable_snapshot_count", restorableSnapshotCount());
            builder.endObject();
        }
        if (exception() != null) {
            builder.startObject("exception")
                .value((bb, pp) -> ElasticsearchException.generateFailureXContent(bb, pp, exception(), true))
                .field("status", ExceptionsHelper.status(exception()))
                .endObject();
        }
        return builder;
    }

    static class Builder {
        private final Writer responseWriter;
        private final Type type;
        private final long timestampMillis;

        private String anomaly;
        private SnapshotId snapshotId;
        private SnapshotInfo snapshotInfo;
        private IndexDescription indexDescription;
        private int shardId = -1;
        private ShardGeneration shardGeneration;
        private String blobName;
        private String physicalFileName;
        private int partIndex = -1;
        private int partCount = -1;
        private ByteSizeValue fileLength = ByteSizeValue.MINUS_ONE;
        private ByteSizeValue partLength = ByteSizeValue.MINUS_ONE;
        private ByteSizeValue blobLength = ByteSizeValue.MINUS_ONE;
        private int totalSnapshotCount = -1;
        private int restorableSnapshotCount = -1;
        private Exception exception;

        Builder(Writer responseWriter, Type type, long timestampMillis) {
            this.responseWriter = responseWriter;
            this.type = type;
            this.timestampMillis = timestampMillis;
        }

        Builder anomaly(String anomaly) {
            this.anomaly = anomaly;
            return this;
        }

        Builder snapshotId(SnapshotId snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        Builder snapshotInfo(SnapshotInfo snapshotInfo) {
            this.snapshotInfo = snapshotInfo;
            return this;
        }

        Builder indexDescription(IndexDescription indexDescription) {
            this.indexDescription = indexDescription;
            return this;
        }

        Builder shardDescription(IndexDescription indexDescription, int shardId) {
            this.indexDescription = indexDescription;
            this.shardId = shardId;
            return this;
        }

        Builder shardGeneration(ShardGeneration shardGeneration) {
            this.shardGeneration = shardGeneration;
            return this;
        }

        Builder blobName(String blobName, String physicalFileName) {
            this.blobName = blobName;
            this.physicalFileName = physicalFileName;
            return this;
        }

        Builder physicalFileName(String physicalFileName) {
            this.physicalFileName = physicalFileName;
            return this;
        }

        Builder part(int partIndex, int partCount) {
            this.partIndex = partIndex;
            this.partCount = partCount;
            return this;
        }

        Builder fileLength(ByteSizeValue fileLength) {
            this.fileLength = Objects.requireNonNull(fileLength);
            return this;
        }

        Builder partLength(ByteSizeValue partLength) {
            this.partLength = Objects.requireNonNull(partLength);
            return this;
        }

        Builder blobLength(ByteSizeValue blobLength) {
            this.blobLength = Objects.requireNonNull(blobLength);
            return this;
        }

        Builder indexRestorability(IndexId indexId, int totalSnapshotCount, int restorableSnapshotCount) {
            this.indexDescription = new IndexDescription(indexId, null, 0);
            this.totalSnapshotCount = totalSnapshotCount;
            this.restorableSnapshotCount = restorableSnapshotCount;
            return this;
        }

        Builder exception(Exception exception) {
            this.exception = exception;
            return this;
        }

        void write(ActionListener<Void> listener) {
            responseWriter.writeResponseChunk(
                new RepositoryVerifyIntegrityResponseChunk(
                    timestampMillis,
                    type,
                    anomaly,
                    snapshotId,
                    snapshotInfo,
                    indexDescription,
                    shardId,
                    shardGeneration,
                    blobName,
                    physicalFileName,
                    partIndex,
                    partCount,
                    fileLength,
                    partLength,
                    blobLength,
                    totalSnapshotCount,
                    restorableSnapshotCount,
                    exception
                ),
                ActionListener.assertOnce(listener)
            );
        }
    }

    interface Writer {
        void writeResponseChunk(RepositoryVerifyIntegrityResponseChunk responseChunk, ActionListener<Void> listener);
    }
}
