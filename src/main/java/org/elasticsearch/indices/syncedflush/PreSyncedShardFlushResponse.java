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

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
class PreSyncedShardFlushResponse extends BroadcastShardOperationResponse {
    byte[] id;
    private ShardRouting shardRouting;

    PreSyncedShardFlushResponse() {
    }

    PreSyncedShardFlushResponse(byte[] id, ShardRouting shardRouting) {
        super(shardRouting.shardId());
        this.id = id;
        this.shardRouting = shardRouting;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readByteArray();
        shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(id);
        shardRouting.writeTo(out);
    }

    byte[] id() {
        return id;
    }

    public ShardRouting shardRouting() {
        return shardRouting;
    }
}
