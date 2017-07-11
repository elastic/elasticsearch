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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.GlobalCheckpointTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * The request object to handoff the primary context to the relocation target.
 */
class RecoveryHandoffPrimaryContextRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private GlobalCheckpointTracker.PrimaryContext primaryContext;

    /**
     * Initialize an empty request (used to serialize into when reading from a stream).
     */
    RecoveryHandoffPrimaryContextRequest() {
    }

    /**
     * Initialize a request for the specified relocation.
     *
     * @param recoveryId     the recovery ID of the relocation
     * @param shardId        the shard ID of the relocation
     * @param primaryContext the primary context
     */
    RecoveryHandoffPrimaryContextRequest(final long recoveryId, final ShardId shardId,
                                         final GlobalCheckpointTracker.PrimaryContext primaryContext) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.primaryContext = primaryContext;
    }

    long recoveryId() {
        return this.recoveryId;
    }

    ShardId shardId() {
        return shardId;
    }

    GlobalCheckpointTracker.PrimaryContext primaryContext() {
        return primaryContext;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        primaryContext = new GlobalCheckpointTracker.PrimaryContext(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        primaryContext.writeTo(out);
    }

    @Override
    public String toString() {
        return "RecoveryHandoffPrimaryContextRequest{" +
                "recoveryId=" + recoveryId +
                ", shardId=" + shardId +
                ", primaryContext=" + primaryContext +
                '}';
    }
}
