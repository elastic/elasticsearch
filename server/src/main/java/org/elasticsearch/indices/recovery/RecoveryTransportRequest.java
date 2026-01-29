/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

public abstract class RecoveryTransportRequest extends AbstractTransportRequest {

    private final long requestSeqNo;

    private final long recoveryId;

    private final ShardId shardId;

    RecoveryTransportRequest(StreamInput in) throws IOException {
        super(in);
        requestSeqNo = in.readLong();
        recoveryId = in.readLong();
        shardId = new ShardId(in);
    }

    RecoveryTransportRequest(long requestSeqNo, long recoveryId, ShardId shardId) {
        this.requestSeqNo = requestSeqNo;
        this.recoveryId = recoveryId;
        this.shardId = shardId;
    }

    public long requestSeqNo() {
        return requestSeqNo;
    }

    public final long recoveryId() {
        return this.recoveryId;
    }

    public final ShardId shardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(requestSeqNo);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
    }
}
