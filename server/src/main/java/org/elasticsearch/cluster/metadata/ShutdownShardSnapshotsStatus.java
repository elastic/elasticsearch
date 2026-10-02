/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracks the status of shard snapshots on a node that is shutting down.
 */
public class ShutdownShardSnapshotsStatus implements Writeable, ToXContentObject {

    public static final TransportVersion SHUTDOWN_SHARD_SNAPSHOTS_STATUS = TransportVersion.fromName("shutdown_shard_snapshots_status");

    public static final ShutdownShardSnapshotsStatus NOT_STARTED = new ShutdownShardSnapshotsStatus(
        SingleNodeShutdownMetadata.Status.NOT_STARTED,
        0L,
        0L,
        0L
    );

    private final SingleNodeShutdownMetadata.Status status;
    private final long completedShards;
    private final long pausedShards;
    private final long runningShards;

    private ShutdownShardSnapshotsStatus(
        SingleNodeShutdownMetadata.Status status,
        long completedShards,
        long pausedShards,
        long runningShards
    ) {
        this.status = status;
        this.completedShards = completedShards;
        this.pausedShards = pausedShards;
        this.runningShards = runningShards;
        assert assertStateConsistency();
    }

    private boolean assertStateConsistency() {
        assert (status == SingleNodeShutdownMetadata.Status.NOT_STARTED && completedShards == 0 && pausedShards == 0 && runningShards == 0)
            || (status == SingleNodeShutdownMetadata.Status.IN_PROGRESS && runningShards > 0)
            || (status == SingleNodeShutdownMetadata.Status.COMPLETE && runningShards == 0) : this;
        return true;
    }

    public static ShutdownShardSnapshotsStatus fromShardCounts(long completedShards, long pausedShards, long runningShards) {
        final var status = runningShards == 0 ? SingleNodeShutdownMetadata.Status.COMPLETE : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
        return new ShutdownShardSnapshotsStatus(status, completedShards, pausedShards, runningShards);
    }

    public static ShutdownShardSnapshotsStatus readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SHUTDOWN_SHARD_SNAPSHOTS_STATUS)) {
            return new ShutdownShardSnapshotsStatus(
                in.readEnum(SingleNodeShutdownMetadata.Status.class),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        } else {
            return fromShardCounts(0, 0, 0);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SHUTDOWN_SHARD_SNAPSHOTS_STATUS)) {
            out.writeEnum(status);
            out.writeVLong(completedShards);
            out.writeVLong(pausedShards);
            out.writeVLong(runningShards);
        }
    }

    public SingleNodeShutdownMetadata.Status status() {
        return status;
    }

    public long completedShards() {
        return completedShards;
    }

    public long pausedShards() {
        return pausedShards;
    }

    public long runningShards() {
        return runningShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.field("completed_shards", completedShards);
        builder.field("paused_shards", pausedShards);
        builder.field("running_shards", runningShards);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ShutdownShardSnapshotsStatus other = (ShutdownShardSnapshotsStatus) obj;
        return completedShards == other.completedShards
            && pausedShards == other.pausedShards
            && runningShards == other.runningShards
            && status == other.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, completedShards, pausedShards, runningShards);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
