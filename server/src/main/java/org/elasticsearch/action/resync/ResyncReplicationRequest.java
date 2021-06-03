/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a batch of operations sent from the primary to its replicas during the primary-replica resync.
 */
public final class ResyncReplicationRequest extends ReplicatedWriteRequest<ResyncReplicationRequest> {

    private final long trimAboveSeqNo;
    private final Translog.Operation[] operations;
    private final long maxSeenAutoIdTimestampOnPrimary;

    ResyncReplicationRequest(StreamInput in) throws IOException {
        super(in);
        trimAboveSeqNo = in.readZLong();
        maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
    }

    public ResyncReplicationRequest(final ShardId shardId, final long trimAboveSeqNo, final long maxSeenAutoIdTimestampOnPrimary,
                                    final Translog.Operation[]operations) {
        super(shardId);
        this.trimAboveSeqNo = trimAboveSeqNo;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.operations = operations;
    }

    public long getTrimAboveSeqNo() {
        return trimAboveSeqNo;
    }

    public long getMaxSeenAutoIdTimestampOnPrimary() {
        return maxSeenAutoIdTimestampOnPrimary;
    }

    public Translog.Operation[] getOperations() {
        return operations;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(trimAboveSeqNo);
        out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        out.writeArray(Translog.Operation::writeOperation, operations);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ResyncReplicationRequest that = (ResyncReplicationRequest) o;
        return trimAboveSeqNo == that.trimAboveSeqNo && maxSeenAutoIdTimestampOnPrimary == that.maxSeenAutoIdTimestampOnPrimary
            && Arrays.equals(operations, that.operations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trimAboveSeqNo, maxSeenAutoIdTimestampOnPrimary, operations);
    }

    @Override
    public String toString() {
        return "TransportResyncReplicationAction.Request{" +
            "shardId=" + shardId +
            ", timeout=" + timeout +
            ", index='" + index + '\'' +
            ", trimAboveSeqNo=" + trimAboveSeqNo +
            ", maxSeenAutoIdTimestampOnPrimary=" + maxSeenAutoIdTimestampOnPrimary +
            ", ops=" + operations.length +
            "}";
    }

}
