/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class SplitStateRequest extends MasterNodeRequest<SplitStateRequest> {

    private final ShardId shardId;
    private final long sourcePrimaryTerm;
    private final long targetPrimaryTerm;
    private final IndexReshardingState.Split.TargetShardState newTargetShardState;

    public SplitStateRequest(
        ShardId shardId,
        IndexReshardingState.Split.TargetShardState newTargetShardState,
        long sourcePrimaryTerm,
        long targetPrimaryTerm
    ) {
        super(INFINITE_MASTER_NODE_TIMEOUT);
        this.shardId = shardId;
        this.newTargetShardState = newTargetShardState;
        this.sourcePrimaryTerm = sourcePrimaryTerm;
        this.targetPrimaryTerm = targetPrimaryTerm;
    }

    public SplitStateRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        newTargetShardState = IndexReshardingState.Split.TargetShardState.readFrom(in);
        sourcePrimaryTerm = in.readVLong();
        targetPrimaryTerm = in.readVLong();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        newTargetShardState.writeTo(out);
        out.writeVLong(sourcePrimaryTerm);
        out.writeVLong(targetPrimaryTerm);
    }

    public long getSourcePrimaryTerm() {
        return sourcePrimaryTerm;
    }

    public long getTargetPrimaryTerm() {
        return targetPrimaryTerm;
    }

    public IndexReshardingState.Split.TargetShardState getNewTargetShardState() {
        return newTargetShardState;
    }
}
