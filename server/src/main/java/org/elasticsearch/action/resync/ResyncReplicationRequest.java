/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.RawIndexingDataTransportRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a batch of operations sent from the primary to its replicas during the primary-replica resync.
 */
public final class ResyncReplicationRequest extends ReplicatedWriteRequest<ResyncReplicationRequest>
    implements RawIndexingDataTransportRequest {

    private final long trimAboveSeqNo;
    private final Translog.Operation[] operations;
    private final long maxSeenAutoIdTimestampOnPrimary;

    ResyncReplicationRequest(StreamInput in) throws IOException {
        super(in);
        assert Version.CURRENT.major <= 7;
        if (in.getVersion().equals(Version.V_6_0_0)) {
            /*
             * Resync replication request serialization was broken in 6.0.0 due to the elements of the stream not being prefixed with a
             * byte indicating the type of the operation.
             */
            // TODO: remove this check in 8.0.0 which provides no BWC guarantees with 6.x.
            throw new IllegalStateException("resync replication request serialization is broken in 6.0.0");
        }
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            trimAboveSeqNo = in.readZLong();
        } else {
            trimAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        } else {
            maxSeenAutoIdTimestampOnPrimary = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
        }
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
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeZLong(trimAboveSeqNo);
        }
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        }
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
