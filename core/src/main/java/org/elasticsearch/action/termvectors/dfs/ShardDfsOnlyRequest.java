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

package org.elasticsearch.action.termvectors.dfs;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;

import java.io.IOException;

public class ShardDfsOnlyRequest extends BroadcastShardRequest {

    private ShardSearchTransportRequest shardSearchRequest = new ShardSearchTransportRequest();

    public ShardDfsOnlyRequest() {

    }

    ShardDfsOnlyRequest(ShardRouting shardRouting, int numberOfShards, @Nullable String[] filteringAliases, long nowInMillis, DfsOnlyRequest request) {
        super(shardRouting.shardId(), request);
        this.shardSearchRequest = new ShardSearchTransportRequest(request.getSearchRequest(), shardRouting, numberOfShards,
                filteringAliases, nowInMillis);
    }

    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardSearchRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardSearchRequest.writeTo(out);
    }

}
