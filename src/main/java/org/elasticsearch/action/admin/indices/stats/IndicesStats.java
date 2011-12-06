/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class IndicesStats extends BroadcastOperationResponse implements ToXContent {

    private ShardStats[] shards;

    IndicesStats() {

    }

    IndicesStats(ShardStats[] shards, ClusterState clusterState, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public ShardStats[] shards() {
        return this.shards;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    public ShardStats getAt(int position) {
        return shards[position];
    }

    public IndexStats index(String index) {
        return indices().get(index);
    }

    public Map<String, IndexStats> getIndices() {
        return indices();
    }

    private Map<String, IndexStats> indicesStats;

    public Map<String, IndexStats> indices() {
        if (indicesStats != null) {
            return indicesStats;
        }
        Map<String, IndexStats> indicesStats = Maps.newHashMap();

        Set<String> indices = Sets.newHashSet();
        for (ShardStats shard : shards) {
            indices.add(shard.index());
        }

        for (String index : indices) {
            List<ShardStats> shards = Lists.newArrayList();
            for (ShardStats shard : this.shards) {
                if (shard.shardRouting().index().equals(index)) {
                    shards.add(shard);
                }
            }
            indicesStats.put(index, new IndexStats(index, shards.toArray(new ShardStats[shards.size()])));
        }
        this.indicesStats = indicesStats;
        return indicesStats;
    }

    private CommonStats total = null;

    public CommonStats getTotal() {
        return total();
    }

    public CommonStats total() {
        if (total != null) {
            return total;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            stats.add(shard.stats());
        }
        total = stats;
        return stats;
    }

    private CommonStats primary = null;

    public CommonStats getPrimaries() {
        return primaries();
    }

    public CommonStats primaries() {
        if (primary != null) {
            return primary;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            if (shard.shardRouting().primary()) {
                stats.add(shard.stats());
            }
        }
        primary = stats;
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = new ShardStats[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = ShardStats.readShardStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shards.length);
        for (ShardStats shard : shards) {
            shard.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("_all");

        builder.startObject("primaries");
        primaries().toXContent(builder, params);
        builder.endObject();

        builder.startObject("total");
        total().toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.INDICES);
        for (IndexStats indexStats : indices().values()) {
            builder.startObject(indexStats.index(), XContentBuilder.FieldCaseConversion.NONE);

            builder.startObject("primaries");
            indexStats.primaries().toXContent(builder, params);
            builder.endObject();

            builder.startObject("total");
            indexStats.total().toXContent(builder, params);
            builder.endObject();

            if ("shards".equalsIgnoreCase(params.param("level", null))) {
                builder.startObject(Fields.SHARDS);
                for (IndexShardStats indexShardStats : indexStats) {
                    builder.startArray(Integer.toString(indexShardStats.shardId().id()));
                    for (ShardStats shardStats : indexShardStats) {
                        builder.startObject();

                        builder.startObject(Fields.ROUTING)
                                .field(Fields.STATE, shardStats.shardRouting().state())
                                .field(Fields.PRIMARY, shardStats.shardRouting().primary())
                                .field(Fields.NODE, shardStats.shardRouting().currentNodeId())
                                .field(Fields.RELOCATING_NODE, shardStats.shardRouting().relocatingNodeId())
                                .endObject();

                        shardStats.stats().toXContent(builder, params);

                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }

            builder.endObject();
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString STATE = new XContentBuilderString("state");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString RELOCATING_NODE = new XContentBuilderString("relocating_node");
    }
}
