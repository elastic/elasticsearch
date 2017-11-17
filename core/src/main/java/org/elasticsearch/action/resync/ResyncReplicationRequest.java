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

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ResyncReplicationRequest extends ReplicatedWriteRequest<ResyncReplicationRequest> {

    private List<Translog.Operation> operations;

    ResyncReplicationRequest() {
        super();
    }

    public ResyncReplicationRequest(ShardId shardId, List<Translog.Operation> operations) {
        super(shardId);
        this.operations = operations;
    }

    public List<Translog.Operation> getOperations() {
        return operations;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        final int size = in.readVInt();
        operations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            operations.add(Translog.Operation.readType(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(operations.size());
        for (int i = 0; i < operations.size(); i++) {
            Translog.Operation.writeType(operations.get(i), out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResyncReplicationRequest that = (ResyncReplicationRequest) o;
        return Objects.equals(getOperations(), that.getOperations());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOperations());
    }

    @Override
    public String toString() {
        return "TransportResyncReplicationAction.Request{" +
            "shardId=" + shardId +
            ", timeout=" + timeout +
            ", index='" + index + '\'' +
            ", ops=" + operations.size() +
            "}";
    }
}
