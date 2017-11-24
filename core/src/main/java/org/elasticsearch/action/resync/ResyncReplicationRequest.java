/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Arrays;

/**
 * Represents a batch of operations sent from the primary to its replicas during the primary-replica resync.
 */
public final class ResyncReplicationRequest extends ReplicatedWriteRequest<ResyncReplicationRequest> {

    private Translog.Operation[] operations;

    ResyncReplicationRequest() {
        super();
    }

    public ResyncReplicationRequest(final ShardId shardId, final Translog.Operation[] operations) {
        super(shardId);
        this.operations = operations;
    }

    public Translog.Operation[] getOperations() {
        return operations;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        if (in.getVersion().equals(Version.V_6_0_0)) {
            /*
             * Resync replication request serialization was broken in 6.0.0 due to the elements of the stream not being prefixed with a
             * byte indicating the type of the operation.
             */
            throw new IllegalStateException("resync replication request serialization is broken in 6.0.0");
        }
        super.readFrom(in);
        operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(Translog.Operation::writeOperation, operations);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ResyncReplicationRequest that = (ResyncReplicationRequest) o;
        return Arrays.equals(operations, that.operations);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(operations);
    }

    @Override
    public String toString() {
        return "TransportResyncReplicationAction.Request{" +
            "shardId=" + shardId +
            ", timeout=" + timeout +
            ", index='" + index + '\'' +
            ", ops=" + operations.length +
            "}";
    }

}
