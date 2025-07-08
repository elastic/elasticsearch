/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RepositoriesStats implements Writeable, ToXContentFragment {

    private final Map<String, SnapshotStats> repositorySnapshotStats;

    public RepositoriesStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            repositorySnapshotStats = in.readMap(SnapshotStats::readFrom);
        } else {
            repositorySnapshotStats = new HashMap<>();
        }
    }

    public RepositoriesStats(Map<String, SnapshotStats> repositorySnapshotStats) {
        this.repositorySnapshotStats = new HashMap<>(repositorySnapshotStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeMap(repositorySnapshotStats, StreamOutput::writeWriteable);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repositories", repositorySnapshotStats);
        return builder;
    }

    public Map<String, SnapshotStats> getRepositorySnapshotStats() {
        return Collections.unmodifiableMap(repositorySnapshotStats);
    }

    public record SnapshotStats(
        long shardSnapshotsStarted,
        long shardSnapshotsCompleted,
        long shardSnapshotsInProgress,
        long totalReadThrottledNanos,
        long totalWriteThrottledNanos,
        long numberOfBlobsUploaded,
        long numberOfBytesUploaded,
        long totalUploadTimeInMillis,
        long totalUploadReadTimeInMillis
    ) implements ToXContentObject, Writeable {

        public static SnapshotStats readFrom(StreamInput in) throws IOException {
            final long totalReadThrottledNanos = in.readVLong();
            final long totalWriteThrottledNanos = in.readVLong();
            if (in.getTransportVersion().onOrAfter(TransportVersions.EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO)) {
                return new SnapshotStats(
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    totalReadThrottledNanos,
                    totalWriteThrottledNanos,
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong()
                );
            } else {
                return new SnapshotStats(totalReadThrottledNanos, totalWriteThrottledNanos);
            }
        }

        public SnapshotStats(long totalReadThrottledNanos, long totalWriteThrottledNanos) {
            this(-1, -1, -1, totalReadThrottledNanos, totalWriteThrottledNanos, -1, -1, -1, -1);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (builder.humanReadable()) {
                builder.field("total_read_throttled_time", new TimeValue(totalReadThrottledNanos, TimeUnit.NANOSECONDS));
                builder.field("total_write_throttled_time", new TimeValue(totalWriteThrottledNanos, TimeUnit.NANOSECONDS));
            }
            builder.field("total_read_throttled_time_nanos", totalReadThrottledNanos);
            builder.field("total_write_throttled_time_nanos", totalWriteThrottledNanos);
            if (shardSnapshotsStarted != -1) {
                builder.field("shard_snapshots_started", shardSnapshotsStarted);
            }
            if (shardSnapshotsCompleted != -1) {
                builder.field("shard_snapshots_completed", shardSnapshotsCompleted);
            }
            if (shardSnapshotsInProgress != -1) {
                builder.field("shard_snapshots_in_progress", shardSnapshotsInProgress);
            }
            if (numberOfBlobsUploaded != -1) {
                builder.field("uploaded_blobs", numberOfBlobsUploaded);
            }
            if (numberOfBytesUploaded != -1) {
                builder.humanReadableField("uploaded_size_in_bytes", "uploaded_size", ByteSizeValue.ofBytes(numberOfBytesUploaded));
            }
            if (totalUploadTimeInMillis != -1) {
                builder.humanReadableField(
                    "total_upload_time_in_millis",
                    "total_upload_time",
                    TimeValue.timeValueMillis(totalUploadTimeInMillis)
                );
            }
            if (totalUploadReadTimeInMillis != -1) {
                builder.humanReadableField(
                    "total_read_time_in_millis",
                    "total_read_time",
                    TimeValue.timeValueMillis(totalUploadReadTimeInMillis)
                );
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalReadThrottledNanos);
            out.writeVLong(totalWriteThrottledNanos);
            if (out.getTransportVersion().onOrAfter(TransportVersions.EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO)) {
                out.writeLong(shardSnapshotsStarted);
                out.writeLong(shardSnapshotsCompleted);
                out.writeLong(shardSnapshotsInProgress);
                out.writeLong(numberOfBlobsUploaded);
                out.writeLong(numberOfBytesUploaded);
                out.writeLong(totalUploadTimeInMillis);
                out.writeLong(totalUploadReadTimeInMillis);
            }
        }
    }
}
