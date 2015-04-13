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

package org.elasticsearch.action.admin.indices.optimize;


import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
final class ShardOptimizeRequest extends BroadcastShardOperationRequest {

    private OptimizeRequest request = new OptimizeRequest();

    ShardOptimizeRequest() {
    }

    ShardOptimizeRequest(ShardId shardId, OptimizeRequest request) {
        super(shardId, request);
        this.request = request;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public OptimizeRequest optimizeRequest() {
        return this.request;
    }
}
