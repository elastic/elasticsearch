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

package org.elasticsearch.action.admin.indices.recovery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class IndicesRecoveryResponse extends BroadcastOperationResponse implements ToXContent {

    private ShardRecoveryStatus[] shardRecoveries;
    private Map<String, List<ShardRecoveryStatus>> indexShardRecoveryMap;

    IndicesRecoveryResponse() {
    }

    IndicesRecoveryResponse(ShardRecoveryStatus[] shardRecoveries, ClusterState clusterState, int totalShards, int successfulShards,
                            int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardRecoveries = shardRecoveries;
    }

    public Map<String, List<ShardRecoveryStatus>> getIndices() {
        if (indexShardRecoveryMap != null) {
            return indexShardRecoveryMap;
        }

        Map<String, List<ShardRecoveryStatus>> indexShardRecoveryMap = newHashMap();
        Set<String> indices = Sets.newHashSet();

        for (ShardRecoveryStatus shardRecoveryStatus : shardRecoveries) {
            indices.add(shardRecoveryStatus.getIndex());
        }

        for (String index : indices) {
            List<ShardRecoveryStatus> recoveringShards = newArrayList();
            for (ShardRecoveryStatus shardRecoveryStatus : shardRecoveries) {
                if (shardRecoveryStatus.getShardRouting().index().equals(index)) {
                    recoveringShards.add(shardRecoveryStatus);
                }
            }

            indexShardRecoveryMap.put(index, recoveringShards);
        }

        this.indexShardRecoveryMap = indexShardRecoveryMap;
        return indexShardRecoveryMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject(Fields.INDICES);
        Map<String, List<ShardRecoveryStatus>> indexShardMap = getIndices();

        for (Map.Entry<String, List<ShardRecoveryStatus>> entry : indexShardMap.entrySet()) {
            builder.startObject(entry.getKey());
            builder.startArray(Fields.SHARDS);
            for (ShardRecoveryStatus status : entry.getValue()) {
                status.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardRecoveries.length);
        for (ShardRecoveryStatus recoveryStatus : shardRecoveries) {
            recoveryStatus.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRecoveries = new ShardRecoveryStatus[in.readVInt()];
        for (int i = 0; i < shardRecoveries.length; i++) {
            ShardRecoveryStatus recoveryStatus = new ShardRecoveryStatus();
            recoveryStatus.readFrom(in);
            shardRecoveries[i] = recoveryStatus;
        }
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
    }
}
