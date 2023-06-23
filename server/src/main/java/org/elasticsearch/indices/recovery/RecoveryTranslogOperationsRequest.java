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
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.RawIndexingDataTransportRequest;

import java.io.IOException;
import java.util.List;

public class RecoveryTranslogOperationsRequest extends RecoveryTransportRequest implements RawIndexingDataTransportRequest {

    private final List<Translog.Operation> operations;
    private final int totalTranslogOps;
    private final long maxSeenAutoIdTimestampOnPrimary;
    private final long maxSeqNoOfUpdatesOrDeletesOnPrimary;
    private final RetentionLeases retentionLeases;
    private final long mappingVersionOnPrimary;

    RecoveryTranslogOperationsRequest(
        final long recoveryId,
        final long requestSeqNo,
        final ShardId shardId,
        final List<Translog.Operation> operations,
        final int totalTranslogOps,
        final long maxSeenAutoIdTimestampOnPrimary,
        final long maxSeqNoOfUpdatesOrDeletesOnPrimary,
        final RetentionLeases retentionLeases,
        final long mappingVersionOnPrimary
    ) {
        super(requestSeqNo, recoveryId, shardId);
        this.operations = operations;
        this.totalTranslogOps = totalTranslogOps;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.maxSeqNoOfUpdatesOrDeletesOnPrimary = maxSeqNoOfUpdatesOrDeletesOnPrimary;
        this.retentionLeases = retentionLeases;
        this.mappingVersionOnPrimary = mappingVersionOnPrimary;
    }

    public List<Translog.Operation> operations() {
        return operations;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long maxSeenAutoIdTimestampOnPrimary() {
        return maxSeenAutoIdTimestampOnPrimary;
    }

    public long maxSeqNoOfUpdatesOrDeletesOnPrimary() {
        return maxSeqNoOfUpdatesOrDeletesOnPrimary;
    }

    public RetentionLeases retentionLeases() {
        return retentionLeases;
    }

    /**
     * Returns the mapping version which is at least as up to date as the mapping version that the primary used to index
     * the translog operations in this request. If the mapping version on the replica is not older this version, we should not
     * retry on {@link org.elasticsearch.index.mapper.MapperException}; otherwise we should wait for a new mapping then retry.
     */
    long mappingVersionOnPrimary() {
        return mappingVersionOnPrimary;
    }

    RecoveryTranslogOperationsRequest(StreamInput in) throws IOException {
        super(in);
        operations = Translog.readOperations(in, "recovery");
        totalTranslogOps = in.readVInt();
        maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        maxSeqNoOfUpdatesOrDeletesOnPrimary = in.readZLong();
        retentionLeases = new RetentionLeases(in);
        mappingVersionOnPrimary = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Translog.writeOperations(out, operations);
        out.writeVInt(totalTranslogOps);
        out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        out.writeZLong(maxSeqNoOfUpdatesOrDeletesOnPrimary);
        retentionLeases.writeTo(out);
        out.writeVLong(mappingVersionOnPrimary);
    }
}
