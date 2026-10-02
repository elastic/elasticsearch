/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

public class GetShardSnapshotCommitInfoRequest extends ActionRequest {

    private final Snapshot snapshot;
    private final ShardId shardId;

    public GetShardSnapshotCommitInfoRequest(ShardId shardId, Snapshot snapshot) {
        this.shardId = Objects.requireNonNull(shardId);
        this.snapshot = Objects.requireNonNull(snapshot);
    }

    public GetShardSnapshotCommitInfoRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.snapshot = new Snapshot(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        snapshot.writeTo(out);
    }

    public ShardId shardId() {
        return shardId;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
