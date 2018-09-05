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

    private List<Translog.Operation> operations;

    public BulkShardOperationsRequest() {
    }

    public BulkShardOperationsRequest(final ShardId shardId, final List<Translog.Operation> operations) {
        super(shardId);
        setRefreshPolicy(RefreshPolicy.NONE);
        this.operations = operations;
    }

    public List<Translog.Operation> getOperations() {
        return operations;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        operations = in.readList(Translog.Operation::readOperation);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(operations.size());
        for (Translog.Operation operation : operations) {
            Translog.Operation.writeOperation(out, operation);
        }
    }

    @Override
    public String toString() {
        return "BulkShardOperationsRequest{" +
                "operations=" + operations.size()+
                ", shardId=" + shardId +
                ", timeout=" + timeout +
                ", index='" + index + '\'' +
                ", waitForActiveShards=" + waitForActiveShards +
                '}';
    }

}
