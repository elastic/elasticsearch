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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

public class RecoveryTranslogOperationsRequest extends RecoveryTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
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
            final long mappingVersionOnPrimary) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.operations = operations;
        this.totalTranslogOps = totalTranslogOps;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.maxSeqNoOfUpdatesOrDeletesOnPrimary = maxSeqNoOfUpdatesOrDeletesOnPrimary;
        this.retentionLeases = retentionLeases;
        this.mappingVersionOnPrimary = mappingVersionOnPrimary;
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
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        operations = Translog.readOperations(in, "recovery");
        totalTranslogOps = in.readVInt();
        maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        maxSeqNoOfUpdatesOrDeletesOnPrimary = in.readZLong();
        retentionLeases = new RetentionLeases(in);
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            mappingVersionOnPrimary = in.readVLong();
        } else {
            mappingVersionOnPrimary = Long.MAX_VALUE;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        Translog.writeOperations(out, operations);
        out.writeVInt(totalTranslogOps);
        out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        out.writeZLong(maxSeqNoOfUpdatesOrDeletesOnPrimary);
        retentionLeases.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeVLong(mappingVersionOnPrimary);
        }
    }

    }
