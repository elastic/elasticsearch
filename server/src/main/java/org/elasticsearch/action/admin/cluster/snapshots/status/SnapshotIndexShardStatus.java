/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.SNAPSHOT_INDEX_SHARD_STATUS_DESCRIPTION_ADDED;

public class SnapshotIndexShardStatus extends BroadcastShardResponse implements ToXContentFragment {

    private final SnapshotIndexShardStage stage;

    private final SnapshotStats stats;

    private String nodeId;

    private String failure;

    private String description;

    public SnapshotIndexShardStatus(StreamInput in) throws IOException {
        super(in);
        stage = SnapshotIndexShardStage.fromValue(in.readByte());
        stats = new SnapshotStats(in);
        nodeId = in.readOptionalString();
        failure = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(SNAPSHOT_INDEX_SHARD_STATUS_DESCRIPTION_ADDED)) {
            description = in.readOptionalString();
        }
    }

    SnapshotIndexShardStatus(ShardId shardId, SnapshotIndexShardStage stage) {
        super(shardId);
        this.stage = stage;
        this.stats = new SnapshotStats();
    }

    SnapshotIndexShardStatus(ShardId shardId, IndexShardSnapshotStatus.Copy indexShardStatus) {
        this(shardId, indexShardStatus, null);
    }

    SnapshotIndexShardStatus(ShardId shardId, IndexShardSnapshotStatus.Copy indexShardStatus, String nodeId) {
        super(shardId);
        stage = switch (indexShardStatus.getStage()) {
            case INIT -> SnapshotIndexShardStage.INIT;
            case STARTED -> SnapshotIndexShardStage.STARTED;
            case FINALIZE -> SnapshotIndexShardStage.FINALIZE;
            case DONE -> SnapshotIndexShardStage.DONE;
            case FAILURE -> SnapshotIndexShardStage.FAILURE;
            default -> throw new IllegalArgumentException("Unknown stage type " + indexShardStatus.getStage());
        };
        this.stats = new SnapshotStats(
            indexShardStatus.getStartTime(),
            indexShardStatus.getTotalTime(),
            indexShardStatus.getIncrementalFileCount(),
            indexShardStatus.getTotalFileCount(),
            indexShardStatus.getProcessedFileCount(),
            indexShardStatus.getIncrementalSize(),
            indexShardStatus.getTotalSize(),
            indexShardStatus.getProcessedSize()
        );
        this.failure = indexShardStatus.getFailure();
        this.nodeId = nodeId;
    }

    SnapshotIndexShardStatus(ShardId shardId, SnapshotIndexShardStage stage, SnapshotStats stats, String nodeId, String failure) {
        this(shardId, stage, stats, nodeId, failure, null);
    }

    SnapshotIndexShardStatus(
        ShardId shardId,
        SnapshotIndexShardStage stage,
        SnapshotStats stats,
        String nodeId,
        String failure,
        String description
    ) {
        super(shardId);
        this.stage = stage;
        this.stats = stats;
        this.nodeId = nodeId;
        this.failure = failure;
        this.description = description;
    }

    /**
     * Returns snapshot stage
     */
    public SnapshotIndexShardStage getStage() {
        return stage;
    }

    /**
     * Returns snapshot stats
     */
    public SnapshotStats getStats() {
        return stats;
    }

    /**
     * Returns node id of the node where snapshot is currently running
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns reason for snapshot failure
     */
    public String getFailure() {
        return failure;
    }

    /**
     * Returns the optional description of the data values contained in the {@code stats} field.
     */
    public String getDescription() {
        return description;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(stage.value());
        stats.writeTo(out);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(failure);
        if (out.getTransportVersion().onOrAfter(SNAPSHOT_INDEX_SHARD_STATUS_DESCRIPTION_ADDED)) {
            out.writeOptionalString(description);
        }
    }

    static final class Fields {
        static final String STAGE = "stage";
        static final String REASON = "reason";
        static final String NODE = "node";
        static final String DESCRIPTION = "description";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Integer.toString(getShardId().getId()));
        builder.field(Fields.STAGE, getStage());
        builder.field(SnapshotStats.Fields.STATS, stats, params);
        if (getNodeId() != null) {
            builder.field(Fields.NODE, getNodeId());
        }
        if (getFailure() != null) {
            builder.field(Fields.REASON, getFailure());
        }
        if (getDescription() != null) {
            builder.field(Fields.DESCRIPTION, getDescription());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotIndexShardStatus that = (SnapshotIndexShardStatus) o;
        return stage == that.stage
            && Objects.equals(stats, that.stats)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(failure, that.failure)
            && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        int result = stage != null ? stage.hashCode() : 0;
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
        result = 31 * result + (failure != null ? failure.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
