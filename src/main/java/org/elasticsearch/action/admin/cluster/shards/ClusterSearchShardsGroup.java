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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class ClusterSearchShardsGroup implements Streamable, ToXContent {

    private String index;
    private int shardId;
    ShardRouting[] shards;

    ClusterSearchShardsGroup() {

    }

    public ClusterSearchShardsGroup(String index, int shardId, ShardRouting[] shards) {
        this.index = index;
        this.shardId = shardId;
        this.shards = shards;
    }

    public static ClusterSearchShardsGroup readSearchShardsGroupResponse(StreamInput in) throws IOException {
        ClusterSearchShardsGroup response = new ClusterSearchShardsGroup();
        response.readFrom(in);
        return response;
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public ShardRouting[] getShards() {
        return shards;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        shards = new ShardRouting[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = ImmutableShardRouting.readShardRoutingEntry(in, index, shardId);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeVInt(shards.length);
        for (ShardRouting shardRouting : shards) {
            shardRouting.writeToThin(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (ShardRouting shard : getShards()) {
            shard.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
