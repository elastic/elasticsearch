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

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;

/**
 * Information regarding the recovery state of a shard.
 */
public class ShardRecoveryResponse extends BroadcastShardOperationResponse implements ToXContent {

    RecoveryState recoveryState;

    public ShardRecoveryResponse() { }

    /**
     * Constructs shard recovery information for the given index and shard id.
     *
     * @param shardId   Id of the shard
     */
    ShardRecoveryResponse(ShardId shardId) {
        super(shardId);
    }

    /**
     * Sets the recovery state information for the shard.
     *
     * @param recoveryState Recovery state
     */
    public void recoveryState(RecoveryState recoveryState) {
        this.recoveryState = recoveryState;
    }

    /**
     * Gets the recovery state information for the shard. Null if shard wasn't recovered / recovery didn't start yet.
     *
     * @return  Recovery state
     */
    @Nullable
    public RecoveryState recoveryState() {
        return recoveryState;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        recoveryState.toXContent(builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        recoveryState.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryState = RecoveryState.readRecoveryState(in);
    }

    /**
     * Builds a new ShardRecoveryResponse from the give input stream.
     *
     * @param in    Input stream
     * @return      A new ShardRecoveryResponse
     * @throws IOException
     */
    public static ShardRecoveryResponse readShardRecoveryResponse(StreamInput in) throws IOException {
        ShardRecoveryResponse response = new ShardRecoveryResponse();
        response.readFrom(in);
        return response;
    }
}
