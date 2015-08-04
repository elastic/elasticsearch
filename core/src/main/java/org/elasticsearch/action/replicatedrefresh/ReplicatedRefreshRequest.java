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

package org.elasticsearch.action.replicatedrefresh;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 */
public class ReplicatedRefreshRequest extends ReplicationRequest<ReplicatedRefreshRequest> {

    public ShardId getShardId() {
        return shardId;
    }

    public void setShardId(ShardId shardId) {
        this.shardId = shardId;
        index(shardId.index().name());
    }

    private ShardId shardId;

    ReplicatedRefreshRequest() {
    }

    /**
     * Copy constructor that creates a new refresh request that is a copy of the one provided as an argument.
     * The new request will inherit though headers and context from the original request that caused it.
     */
    public ReplicatedRefreshRequest(ActionRequest originalRequest) {
        super(originalRequest);
    }

    public ReplicatedRefreshRequest(ShardId shardId) {
        this.shardId = shardId;
        index(shardId.index().name());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.shardId = ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
    }

    @Override
    public boolean skipExecutionOnShadowReplicas() {
        return false;
    }

}
