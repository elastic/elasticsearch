/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * The request object to handoff the primary context to the relocation target.
 */
class RecoveryHandoffPrimaryContextRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private ReplicationTracker.PrimaryContext primaryContext;

    /**
     * Initialize an empty request (used to serialize into when reading from a stream).
     */
    RecoveryHandoffPrimaryContextRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        primaryContext = new ReplicationTracker.PrimaryContext(in);
    }

    /**
     * Initialize a request for the specified relocation.
     *
     * @param recoveryId     the recovery ID of the relocation
     * @param shardId        the shard ID of the relocation
     * @param primaryContext the primary context
     */
    RecoveryHandoffPrimaryContextRequest(
        final long recoveryId,
        final ShardId shardId,
        final ReplicationTracker.PrimaryContext primaryContext
    ) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.primaryContext = primaryContext;
    }

    long recoveryId() {
        return this.recoveryId;
    }

    ShardId shardId() {
        return shardId;
    }

    ReplicationTracker.PrimaryContext primaryContext() {
        return primaryContext;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        primaryContext.writeTo(out);
    }

    @Override
    public String toString() {
        return "RecoveryHandoffPrimaryContextRequest{"
            + "recoveryId="
            + recoveryId
            + ", shardId="
            + shardId
            + ", primaryContext="
            + primaryContext
            + '}';
    }
}
