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

package org.elasticsearch.action.admin.indices.status;

import com.google.common.collect.Sets;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.action.admin.indices.status.ShardStatus.readIndexShardStatus;

/**
 *
 */
public class IndicesStatusResponse extends BroadcastOperationResponse implements ToXContent {

    protected ShardStatus[] shards;

    private Map<String, IndexStatus> indicesStatus;

    IndicesStatusResponse() {
    }

    IndicesStatusResponse(ShardStatus[] shards, ClusterState clusterState, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public ShardStatus[] getShards() {
        return this.shards;
    }

    public ShardStatus getAt(int position) {
        return shards[position];
    }

    public IndexStatus getIndex(String index) {
        return getIndices().get(index);
    }

    public Map<String, IndexStatus> getIndices() {
        if (indicesStatus != null) {
            return indicesStatus;
        }
        Map<String, IndexStatus> indicesStatus = newHashMap();

        Set<String> indices = Sets.newHashSet();
        for (ShardStatus shard : shards) {
            indices.add(shard.getIndex());
        }

        for (String index : indices) {
            List<ShardStatus> shards = newArrayList();
            for (ShardStatus shard : this.shards) {
                if (shard.getShardRouting().index().equals(index)) {
                    shards.add(shard);
                }
            }
            indicesStatus.put(index, new IndexStatus(index, shards.toArray(new ShardStatus[shards.size()])));
        }
        this.indicesStatus = indicesStatus;
        return indicesStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(getShards().length);
        for (ShardStatus status : getShards()) {
            status.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = new ShardStatus[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = readIndexShardStatus(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, @Nullable SettingsFilter settingsFilter) throws IOException {
        builder.startObject(Fields.INDICES);
        for (IndexStatus indexStatus : getIndices().values()) {
            builder.startObject(indexStatus.getIndex(), XContentBuilder.FieldCaseConversion.NONE);

            builder.startObject(Fields.INDEX);
            if (indexStatus.getStoreSize() != null) {
                builder.byteSizeField(Fields.PRIMARY_SIZE_IN_BYTES, Fields.PRIMARY_SIZE, indexStatus.getPrimaryStoreSize());
                builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, indexStatus.getStoreSize());
            }
            builder.endObject();
            if (indexStatus.getTranslogOperations() != -1) {
                builder.startObject(Fields.TRANSLOG);
                builder.field(Fields.OPERATIONS, indexStatus.getTranslogOperations());
                builder.endObject();
            }

            if (indexStatus.getDocs() != null) {
                builder.startObject(Fields.DOCS);
                builder.field(Fields.NUM_DOCS, indexStatus.getDocs().getNumDocs());
                builder.field(Fields.MAX_DOC, indexStatus.getDocs().getMaxDoc());
                builder.field(Fields.DELETED_DOCS, indexStatus.getDocs().getDeletedDocs());
                builder.endObject();
            }

            MergeStats mergeStats = indexStatus.getMergeStats();
            if (mergeStats != null) {
                mergeStats.toXContent(builder, params);
            }
            RefreshStats refreshStats = indexStatus.getRefreshStats();
            if (refreshStats != null) {
                refreshStats.toXContent(builder, params);
            }
            FlushStats flushStats = indexStatus.getFlushStats();
            if (flushStats != null) {
                flushStats.toXContent(builder, params);
            }

            builder.startObject(Fields.SHARDS);
            for (IndexShardStatus indexShardStatus : indexStatus) {
                builder.startArray(Integer.toString(indexShardStatus.getShardId().id()));
                for (ShardStatus shardStatus : indexShardStatus) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING)
                            .field(Fields.STATE, shardStatus.getShardRouting().state())
                            .field(Fields.PRIMARY, shardStatus.getShardRouting().primary())
                            .field(Fields.NODE, shardStatus.getShardRouting().currentNodeId())
                            .field(Fields.RELOCATING_NODE, shardStatus.getShardRouting().relocatingNodeId())
                            .field(Fields.SHARD, shardStatus.getShardRouting().shardId().id())
                            .field(Fields.INDEX, shardStatus.getShardRouting().shardId().index().name())
                            .endObject();

                    builder.field(Fields.STATE, shardStatus.getState());
                    if (shardStatus.getStoreSize() != null) {
                        builder.startObject(Fields.INDEX);
                        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, shardStatus.getStoreSize());
                        builder.endObject();
                    }
                    if (shardStatus.getTranslogId() != -1) {
                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.ID, shardStatus.getTranslogId());
                        builder.field(Fields.OPERATIONS, shardStatus.getTranslogOperations());
                        builder.endObject();
                    }

                    if (shardStatus.getDocs() != null) {
                        builder.startObject(Fields.DOCS);
                        builder.field(Fields.NUM_DOCS, shardStatus.getDocs().getNumDocs());
                        builder.field(Fields.MAX_DOC, shardStatus.getDocs().getMaxDoc());
                        builder.field(Fields.DELETED_DOCS, shardStatus.getDocs().getDeletedDocs());
                        builder.endObject();
                    }

                    mergeStats = shardStatus.getMergeStats();
                    if (mergeStats != null) {
                        mergeStats.toXContent(builder, params);
                    }

                    refreshStats = shardStatus.getRefreshStats();
                    if (refreshStats != null) {
                        refreshStats.toXContent(builder, params);
                    }
                    flushStats = shardStatus.getFlushStats();
                    if (flushStats != null) {
                        flushStats.toXContent(builder, params);
                    }

                    if (shardStatus.getPeerRecoveryStatus() != null) {
                        PeerRecoveryStatus peerRecoveryStatus = shardStatus.getPeerRecoveryStatus();
                        builder.startObject(Fields.PEER_RECOVERY);
                        builder.field(Fields.STAGE, peerRecoveryStatus.getStage());
                        builder.field(Fields.START_TIME_IN_MILLIS, peerRecoveryStatus.getStartTime());
                        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, peerRecoveryStatus.getTime());

                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.PROGRESS, peerRecoveryStatus.getIndexRecoveryProgress());
                        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, peerRecoveryStatus.getIndexSize());
                        builder.byteSizeField(Fields.REUSED_SIZE_IN_BYTES, Fields.REUSED_SIZE, peerRecoveryStatus.getReusedIndexSize());
                        builder.byteSizeField(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, Fields.EXPECTED_RECOVERED_SIZE, peerRecoveryStatus.getExpectedRecoveredIndexSize());
                        builder.byteSizeField(Fields.RECOVERED_SIZE_IN_BYTES, Fields.RECOVERED_SIZE, peerRecoveryStatus.getRecoveredIndexSize());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.RECOVERED, peerRecoveryStatus.getRecoveredTranslogOperations());
                        builder.endObject();

                        builder.endObject();
                    }

                    if (shardStatus.getGatewayRecoveryStatus() != null) {
                        GatewayRecoveryStatus gatewayRecoveryStatus = shardStatus.getGatewayRecoveryStatus();
                        builder.startObject(Fields.GATEWAY_RECOVERY);
                        builder.field(Fields.STAGE, gatewayRecoveryStatus.getStage());
                        builder.field(Fields.START_TIME_IN_MILLIS, gatewayRecoveryStatus.getStartTime());
                        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, gatewayRecoveryStatus.getTime());

                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.PROGRESS, gatewayRecoveryStatus.getIndexRecoveryProgress());
                        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, gatewayRecoveryStatus.getIndexSize());
                        builder.byteSizeField(Fields.REUSED_SIZE_IN_BYTES, Fields.REUSED_SIZE, gatewayRecoveryStatus.getReusedIndexSize());
                        builder.byteSizeField(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, Fields.EXPECTED_RECOVERED_SIZE, gatewayRecoveryStatus.getExpectedRecoveredIndexSize());
                        builder.byteSizeField(Fields.RECOVERED_SIZE_IN_BYTES, Fields.RECOVERED_SIZE, gatewayRecoveryStatus.getRecoveredIndexSize());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.RECOVERED, gatewayRecoveryStatus.getRecoveredTranslogOperations());
                        builder.endObject();

                        builder.endObject();
                    }

                    if (shardStatus.getGatewaySnapshotStatus() != null) {
                        GatewaySnapshotStatus gatewaySnapshotStatus = shardStatus.getGatewaySnapshotStatus();
                        builder.startObject(Fields.GATEWAY_SNAPSHOT);
                        builder.field(Fields.STAGE, gatewaySnapshotStatus.getStage());
                        builder.field(Fields.START_TIME_IN_MILLIS, gatewaySnapshotStatus.getStartTime());
                        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, gatewaySnapshotStatus.getTime());

                        builder.startObject(Fields.INDEX);
                        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, gatewaySnapshotStatus.getIndexSize());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.EXPECTED_OPERATIONS, gatewaySnapshotStatus.getExpectedNumberOfOperations());
                        builder.endObject();

                        builder.endObject();
                    }

                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString PRIMARY_SIZE = new XContentBuilderString("primary_size");
        static final XContentBuilderString PRIMARY_SIZE_IN_BYTES = new XContentBuilderString("primary_size_in_bytes");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
        static final XContentBuilderString TRANSLOG = new XContentBuilderString("translog");
        static final XContentBuilderString OPERATIONS = new XContentBuilderString("operations");
        static final XContentBuilderString DOCS = new XContentBuilderString("docs");
        static final XContentBuilderString NUM_DOCS = new XContentBuilderString("num_docs");
        static final XContentBuilderString MAX_DOC = new XContentBuilderString("max_doc");
        static final XContentBuilderString DELETED_DOCS = new XContentBuilderString("deleted_docs");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString ROUTING = new XContentBuilderString("routing");
        static final XContentBuilderString STATE = new XContentBuilderString("state");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString RELOCATING_NODE = new XContentBuilderString("relocating_node");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString PEER_RECOVERY = new XContentBuilderString("peer_recovery");
        static final XContentBuilderString STAGE = new XContentBuilderString("stage");
        static final XContentBuilderString START_TIME_IN_MILLIS = new XContentBuilderString("start_time_in_millis");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString TIME_IN_MILLIS = new XContentBuilderString("time_in_millis");
        static final XContentBuilderString PROGRESS = new XContentBuilderString("progress");
        static final XContentBuilderString REUSED_SIZE = new XContentBuilderString("reused_size");
        static final XContentBuilderString REUSED_SIZE_IN_BYTES = new XContentBuilderString("reused_size_in_bytes");
        static final XContentBuilderString EXPECTED_RECOVERED_SIZE = new XContentBuilderString("expected_recovered_size");
        static final XContentBuilderString EXPECTED_RECOVERED_SIZE_IN_BYTES = new XContentBuilderString("expected_recovered_size_in_bytes");
        static final XContentBuilderString RECOVERED_SIZE = new XContentBuilderString("recovered_size");
        static final XContentBuilderString RECOVERED_SIZE_IN_BYTES = new XContentBuilderString("recovered_size_in_bytes");
        static final XContentBuilderString RECOVERED = new XContentBuilderString("recovered");
        static final XContentBuilderString GATEWAY_RECOVERY = new XContentBuilderString("gateway_recovery");
        static final XContentBuilderString GATEWAY_SNAPSHOT = new XContentBuilderString("gateway_snapshot");
        static final XContentBuilderString EXPECTED_OPERATIONS = new XContentBuilderString("expected_operations");
    }
}
