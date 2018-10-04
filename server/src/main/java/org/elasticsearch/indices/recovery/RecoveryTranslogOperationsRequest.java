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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

public class RecoveryTranslogOperationsRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private List<Translog.Operation> operations;
    private int totalTranslogOps = RecoveryState.Translog.UNKNOWN;
    private long maxSeenAutoIdTimestampOnPrimary;
    private long maxSeqNoOfUpdatesOrDeletesOnPrimary;

    public RecoveryTranslogOperationsRequest() {
    }

    RecoveryTranslogOperationsRequest(long recoveryId, ShardId shardId, List<Translog.Operation> operations, int totalTranslogOps,
                                      long maxSeenAutoIdTimestampOnPrimary, long maxSeqNoOfUpdatesOrDeletesOnPrimary) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.operations = operations;
        this.totalTranslogOps = totalTranslogOps;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.maxSeqNoOfUpdatesOrDeletesOnPrimary = maxSeqNoOfUpdatesOrDeletesOnPrimary;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        operations = Translog.readOperations(in, "recovery");
        totalTranslogOps = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        } else {
            maxSeenAutoIdTimestampOnPrimary = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
        }
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            maxSeqNoOfUpdatesOrDeletesOnPrimary = in.readZLong();
        } else {
            // UNASSIGNED_SEQ_NO means uninitialized and replica won't enable optimization using seq_no
            maxSeqNoOfUpdatesOrDeletesOnPrimary = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        Translog.writeOperations(out, operations);
        out.writeVInt(totalTranslogOps);
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        }
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeZLong(maxSeqNoOfUpdatesOrDeletesOnPrimary);
        }
    }
}
