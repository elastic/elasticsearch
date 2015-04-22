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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

/**
 */
public class ShardStats extends BroadcastShardOperationResponse implements ToXContent {

    private ShardRouting shardRouting;

    CommonStats commonStats;

    @Nullable
    CommitStats commitStats;

    ShardStats() {
    }

    public ShardStats(IndexShard indexShard, ShardRouting shardRouting, CommonStatsFlags flags) {
        super(indexShard.shardId());
        this.shardRouting = shardRouting;
        this.commonStats = new CommonStats(indexShard, flags);
        this.commitStats = indexShard.commitStats();
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public CommonStats getStats() {
        return this.commonStats;
    }

    public CommitStats getCommitStats() {
        return this.commitStats;
    }

    public static ShardStats readShardStats(StreamInput in) throws IOException {
        ShardStats stats = new ShardStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        commonStats = CommonStats.readCommonStats(in);
        commitStats = CommitStats.readOptionalCommitStatsFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        commonStats.writeTo(out);
        out.writeOptionalStreamable(commitStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ROUTING)
                .field(Fields.STATE, shardRouting.state())
                .field(Fields.PRIMARY, shardRouting.primary())
                .field(Fields.NODE, shardRouting.currentNodeId())
                .field(Fields.RELOCATING_NODE, shardRouting.relocatingNodeId())
                .endObject();

        commonStats.toXContent(builder, params);
        if (commitStats != null) {
            commitStats.toXContent(builder, params);
        }
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString STATE = new XContentBuilderString("state");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString RELOCATING_NODE = new XContentBuilderString("relocating_node");
    }

}
