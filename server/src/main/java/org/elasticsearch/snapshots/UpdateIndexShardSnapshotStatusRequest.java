/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal request that is used to send changes in snapshot status to master
 */
public class UpdateIndexShardSnapshotStatusRequest extends MasterNodeRequest<UpdateIndexShardSnapshotStatusRequest> {
    private final Snapshot snapshot;
    private final ShardId shardId;
    private final SnapshotsInProgress.ShardSnapshotStatus status;

    public UpdateIndexShardSnapshotStatusRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = new Snapshot(in);
        shardId = new ShardId(in);
        status = SnapshotsInProgress.ShardSnapshotStatus.readFrom(in);
    }

    public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus status) {
        this.snapshot = snapshot;
        this.shardId = shardId;
        this.status = status;
        // By default, we keep trying to post snapshot status messages to avoid snapshot processes getting stuck.
        this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        snapshot.writeTo(out);
        shardId.writeTo(out);
        status.writeTo(out);
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public ShardId shardId() {
        return shardId;
    }

    public SnapshotsInProgress.ShardSnapshotStatus status() {
        return status;
    }

    @Override
    public String toString() {
        return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UpdateIndexShardSnapshotStatusRequest that = (UpdateIndexShardSnapshotStatusRequest) o;
        return snapshot.equals(that.snapshot) && shardId.equals(that.shardId) && status.equals(that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, shardId, status);
    }
}
