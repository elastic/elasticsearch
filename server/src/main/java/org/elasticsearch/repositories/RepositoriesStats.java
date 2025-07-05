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

    private final Map<String, SnapshotStats> repositoryThrottlingStats;

    public RepositoriesStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            repositoryThrottlingStats = in.readMap(SnapshotStats::readFrom);
        } else {
            repositoryThrottlingStats = new HashMap<>();
        }
    }

    public RepositoriesStats(Map<String, SnapshotStats> repositoryThrottlingStats) {
        this.repositoryThrottlingStats = new HashMap<>(repositoryThrottlingStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeMap(repositoryThrottlingStats, StreamOutput::writeWriteable);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repositories", repositoryThrottlingStats);
        return builder;
    }

    public Map<String, SnapshotStats> getRepositoryThrottlingStats() {
        return Collections.unmodifiableMap(repositoryThrottlingStats);
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
                builder.field("blobs_uploaded", numberOfBlobsUploaded);
            }
            if (numberOfBytesUploaded != -1) {
                if (builder.humanReadable()) {
                    builder.field("bytes_uploaded", ByteSizeValue.ofBytes(numberOfBytesUploaded));
                } else {
                    builder.field("bytes_uploaded", numberOfBytesUploaded);
                }
            }
            if (totalUploadTimeInNanos != -1) {
                if (builder.humanReadable()) {
                    builder.field("total_upload_time", TimeValue.timeValueNanos(totalUploadTimeInNanos));
                } else {
                    builder.field("total_upload_time_in_nanos", totalUploadTimeInNanos);
                }
            }
            if (totalUploadReadTimeInNanos != -1) {
                if (builder.humanReadable()) {
                    builder.field("total_read_time", TimeValue.timeValueNanos(totalUploadReadTimeInNanos));
                } else {
                    builder.field("total_read_time_in_nanos", totalUploadReadTimeInNanos);
                }
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
                out.writeLong(totalUploadTimeInNanos);
                out.writeLong(totalUploadReadTimeInNanos);
            }
        }
    }
}
