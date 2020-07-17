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

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class BulkShardResponse extends ReplicationResponse implements WriteResponse {

    private static final Version COMPACT_SHARD_ID_VERSION = Version.V_7_9_0;

    private final ShardId shardId;
    private final BulkItemResponse[] responses;

    BulkShardResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        responses = new BulkItemResponse[in.readVInt()];
        if (in.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION)) {
            for (int i = 0; i < responses.length; i++) {
                responses[i] = new BulkItemResponse(shardId, in);
            }
        } else {
            for (int i = 0; i < responses.length; i++) {
                responses[i] = new BulkItemResponse(in);
            }
        }
    }

    // NOTE: public for testing only
    public BulkShardResponse(ShardId shardId, BulkItemResponse[] responses) {
        this.shardId = shardId;
        this.responses = responses;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public BulkItemResponse[] getResponses() {
        return responses;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        /*
         * Each DocWriteResponse already has a location for whether or not it forced a refresh so we just set that information on the
         * response.
         */
        for (BulkItemResponse response : responses) {
            DocWriteResponse r = response.getResponse();
            if (r != null) {
                r.setForcedRefresh(forcedRefresh);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(responses.length);
        if (out.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION)) {
            for (BulkItemResponse response : responses) {
                response.writeThin(out);
            }
        } else {
            for (BulkItemResponse response : responses) {
                response.writeTo(out);
            }
        }
    }
}
