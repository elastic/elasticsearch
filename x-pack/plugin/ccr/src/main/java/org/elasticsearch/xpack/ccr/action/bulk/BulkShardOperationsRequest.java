/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

public final class BulkShardOperationsRequest extends ReplicatedWriteRequest<BulkShardOperationsRequest> {

    private String expectedHistoryUUID;
    private List<Translog.Operation> operations;
    private long maxSeqNoOfUpdatesOrDeletes;

    public BulkShardOperationsRequest() {
    }

    public BulkShardOperationsRequest(final ShardId shardId,
                                      final String expectedHistoryUUID,
                                      final List<Translog.Operation> operations,
                                      long maxSeqNoOfUpdatesOrDeletes) {
        super(shardId);
        setRefreshPolicy(RefreshPolicy.NONE);
        this.expectedHistoryUUID = expectedHistoryUUID;
        this.operations = operations;
        this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
    }

    public String getExpectedHistoryUUID() {
        return expectedHistoryUUID;
    }

    public List<Translog.Operation> getOperations() {
        return operations;
    }

    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        expectedHistoryUUID = in.readString();
        maxSeqNoOfUpdatesOrDeletes = in.readZLong();
        operations = in.readList(Translog.Operation::readOperation);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(expectedHistoryUUID);
        out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
        out.writeVInt(operations.size());
        for (Translog.Operation operation : operations) {
            Translog.Operation.writeOperation(out, operation);
        }
    }

    @Override
    public String toString() {
        return "BulkShardOperationsRequest{" +
                "expectedHistoryUUID=" + expectedHistoryUUID +
                ", operations=" + operations.size() +
                ", maxSeqNoUpdates=" + maxSeqNoOfUpdatesOrDeletes +
                ", shardId=" + shardId +
                ", timeout=" + timeout +
                ", index='" + index + '\'' +
                ", waitForActiveShards=" + waitForActiveShards +
                '}';
    }

}
