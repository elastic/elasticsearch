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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
public class BulkShardResponse extends ReplicationResponse implements ReplicatedWriteResponse {

    private ShardId shardId;
    private BulkItemResponse[] responses;

    BulkShardResponse() {
    }

    BulkShardResponse(ShardId shardId, BulkItemResponse[] responses) {
        this.shardId = shardId;
        this.responses = responses;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public BulkItemResponse[] getResponses() {
        return responses;
    }

    /**
     * Each DocWriteResponse already has a location for whether or not it forced a refresh so we just set that information on the response.
     *
     * Requests that set {@link WriteRequest#setRefresh(boolean)} to true should always set this to true. Requests that set
     * {@link WriteRequest#setBlockUntilRefresh(boolean)} to true should only set this to true if they run out of refresh
     * listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        for (BulkItemResponse response : responses) {
            DocWriteResponse r = response.getResponse();
            if (r != null) {
                r.setForcedRefresh(forcedRefresh);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        responses = new BulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = BulkItemResponse.readBulkItem(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(responses.length);
        for (BulkItemResponse response : responses) {
            response.writeTo(out);
        }
    }
}
