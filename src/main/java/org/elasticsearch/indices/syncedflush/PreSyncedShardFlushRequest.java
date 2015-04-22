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

package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
class PreSyncedShardFlushRequest extends BroadcastShardOperationRequest {

    private ShardRouting shardRouting;
    // we need our own request because it has to include the shard routing
    private PreSyncedFlushRequest request = new PreSyncedFlushRequest();

    PreSyncedShardFlushRequest() {
    }

    PreSyncedShardFlushRequest(ShardRouting shardRouting, PreSyncedFlushRequest request) {
        super(shardRouting.shardId(), request);
        this.request = request;
        this.shardRouting = shardRouting;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request.readFrom(in);
        shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
        shardRouting.writeTo(out);

    }

    PreSyncedFlushRequest getRequest() {
        return request;
    }

    public ShardRouting shardRouting() {
        return shardRouting;
    }
}
