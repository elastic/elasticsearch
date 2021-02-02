/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

class RecoveryPrepareForTranslogOperationsRequest extends RecoveryTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final int totalTranslogOps;

    RecoveryPrepareForTranslogOperationsRequest(long recoveryId, long requestSeqNo, ShardId shardId, int totalTranslogOps) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.totalTranslogOps = totalTranslogOps;
    }

    RecoveryPrepareForTranslogOperationsRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        totalTranslogOps = in.readVInt();
        if (in.getVersion().before(Version.V_6_0_0_alpha1)) {
            in.readLong(); // maxUnsafeAutoIdTimestamp
        }
        if (in.getVersion().onOrAfter(Version.V_6_2_0) && in.getVersion().before(Version.V_7_4_0)) {
            in.readBoolean(); // was fileBasedRecovery
        }
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeVInt(totalTranslogOps);
        if (out.getVersion().before(Version.V_6_0_0_alpha1)) {
            out.writeLong(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP); // maxUnsafeAutoIdTimestamp
        }
        if (out.getVersion().onOrAfter(Version.V_6_2_0) && out.getVersion().before(Version.V_7_4_0)) {
            out.writeBoolean(true); // was fileBasedRecovery
        }
    }
}
