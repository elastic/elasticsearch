/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersion;
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
        repositorySnapshotStats = in.readMap(SnapshotStats::readFrom);
    }

    public RepositoriesStats(Map<String, SnapshotStats> repositorySnapshotStats) {
        this.repositorySnapshotStats = new HashMap<>(repositorySnapshotStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(repositorySnapshotStats, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repositories", repositorySnapshotStats);
        return builder;
    }

    public Map<String, SnapshotStats> getRepositorySnapshotStats() {
        return Collections.unmodifiableMap(repositorySnapshotStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoriesStats that = (RepositoriesStats) o;
        return repositorySnapshotStats.equals(that.repositorySnapshotStats);
    }

    @Override
    public int hashCode() {
        return repositorySnapshotStats.hashCode();
    }

    public record SnapshotStats(
        long shardSnapshotsStarted,
        long shardSnapshotsCompleted,
        long shardSnapshotsInProgress,
        long totalReadThrottledNanos,
        long totalWriteThrottledNanos,
        long numberOfBlobsUploaded,
        long numberOfBytesUploaded,
        long totalUploadTimeInNanos,
        long totalUploadReadTimeInNanos
    ) implements ToXContentObject, Writeable {

        private static final TransportVersion EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO = TransportVersion.fromName(
            "extended_snapshot_stats_in_node_info"
        );
        static final TransportVersion UPLOAD_TIME_NANOS = TransportVersion.fromName("snapshot_upload_time_nanos");

        public static final SnapshotStats ZERO = new SnapshotStats(0, 0);

        public static SnapshotStats readFrom(StreamInput in) throws IOException {
            final long totalReadThrottledNanos = in.readVLong();
            final long totalWriteThrottledNanos = in.readVLong();
            if (in.getTransportVersion().supports(EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO)) {
                final long shardSnapshotsStarted = in.readVLong();
                final long shardSnapshotsCompleted = in.readVLong();
                final long shardSnapshotsInProgress = in.readVLong();
                final long numberOfBlobsUploaded = in.readVLong();
                final long numberOfBytesUploaded = in.readVLong();

                final long uploadTimeNanos;
                final long uploadReadTimeNanos;
                // If we support nanoseconds, then no need to modify the value
                if (in.getTransportVersion().supports(UPLOAD_TIME_NANOS)) {
                    uploadTimeNanos = in.readVLong();
                    uploadReadTimeNanos = in.readVLong();
                }
                // For older transport versions, we have to convert the milliseconds into nanoseconds
                else {
                    uploadTimeNanos = TimeUnit.MILLISECONDS.toNanos(in.readVLong());
                    uploadReadTimeNanos = TimeUnit.MILLISECONDS.toNanos(in.readVLong());
                }

                return new SnapshotStats(
                    shardSnapshotsStarted,
                    shardSnapshotsCompleted,
                    shardSnapshotsInProgress,
                    totalReadThrottledNanos,
                    totalWriteThrottledNanos,
                    numberOfBlobsUploaded,
                    numberOfBytesUploaded,
                    uploadTimeNanos,
                    uploadReadTimeNanos
                );
            } else {
                return new SnapshotStats(totalReadThrottledNanos, totalWriteThrottledNanos);
            }
        }

        public SnapshotStats(long totalReadThrottledNanos, long totalWriteThrottledNanos) {
            this(0, 0, 0, totalReadThrottledNanos, totalWriteThrottledNanos, 0, 0, 0, 0);
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
            builder.field("shard_snapshots_started", shardSnapshotsStarted);
            builder.field("shard_snapshots_completed", shardSnapshotsCompleted);
            builder.field("shard_snapshots_in_progress", shardSnapshotsInProgress);
            builder.field("uploaded_blobs", numberOfBlobsUploaded);
            builder.humanReadableField("uploaded_size_in_bytes", "uploaded_size", ByteSizeValue.ofBytes(numberOfBytesUploaded));
            builder.humanReadableField(
                "total_upload_time_in_millis",
                "total_upload_time",
                TimeValue.timeValueNanos(totalUploadTimeInNanos).millis()
            );
            builder.humanReadableField(
                "total_read_time_in_millis",
                "total_read_time",
                TimeValue.timeValueNanos(totalUploadReadTimeInNanos).millis()
            );
            builder.humanReadableField("total_upload_time_in_nanos", "total_upload_time_nanos", totalUploadTimeInNanos);
            builder.humanReadableField("total_read_time_in_nanos", "total_read_time_nanos", totalUploadReadTimeInNanos);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalReadThrottledNanos);
            out.writeVLong(totalWriteThrottledNanos);
            if (out.getTransportVersion().supports(EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO)) {
                out.writeVLong(shardSnapshotsStarted);
                out.writeVLong(shardSnapshotsCompleted);
                out.writeVLong(shardSnapshotsInProgress);
                out.writeVLong(numberOfBlobsUploaded);
                out.writeVLong(numberOfBytesUploaded);

                // If we support nanoseconds, then no need to modify the value
                if (out.getTransportVersion().supports(UPLOAD_TIME_NANOS)) {
                    out.writeVLong(totalUploadTimeInNanos);
                    out.writeVLong(totalUploadReadTimeInNanos);
                }
                // For older transport versions, we have to convert the nanoseconds back to milliseconds
                else {
                    out.writeVLong(TimeUnit.NANOSECONDS.toMillis(totalUploadTimeInNanos));
                    out.writeVLong(TimeUnit.NANOSECONDS.toMillis(totalUploadReadTimeInNanos));
                }
            }
        }
    }
}
