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

import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A response of an index operation,
 *
 * @see ReplicatedRefreshRequest
 */
public class ReplicatedRefreshResponse extends ActionWriteResponse {

    public ShardId getShardId() {
        return shardId;
    }

    public void setShardId(ShardId shardId) {
        this.shardId = shardId;
    }

    private ShardId shardId;

    private int totalNumCopies;


    public ReplicatedRefreshResponse() {
    }

    public ReplicatedRefreshResponse(ShardId shardId, int totalNumCopies) {
        this.shardId = shardId;
        this.totalNumCopies = totalNumCopies;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        totalNumCopies = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(totalNumCopies);
    }

    @Override
    public String toString() {
        return "ReplicatedRefreshResponse{" +
                "shardId=" + shardId +
                ", totalNumCopies=" + totalNumCopies +
                '}';
    }

    public int getTotalNumCopies() {
        return totalNumCopies;
    }
}
