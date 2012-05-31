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

    public ShardStatus[] shards() {
        return this.shards;
    }

    public ShardStatus[] getShards() {
        return this.shards;
    }

    public ShardStatus getAt(int position) {
        return shards[position];
    }

    public IndexStatus index(String index) {
        return indices().get(index);
    }

    public Map<String, IndexStatus> getIndices() {
        return indices();
    }

    public Map<String, IndexStatus> indices() {
        if (indicesStatus != null) {
            return indicesStatus;
        }
        Map<String, IndexStatus> indicesStatus = newHashMap();

        Set<String> indices = Sets.newHashSet();
        for (ShardStatus shard : shards) {
            indices.add(shard.index());
        }

        for (String index : indices) {
            List<ShardStatus> shards = newArrayList();
            for (ShardStatus shard : this.shards) {
                if (shard.shardRouting().index().equals(index)) {
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
        out.writeVInt(shards().length);
        for (ShardStatus status : shards()) {
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
        for (IndexStatus indexStatus : indices().values()) {
            builder.startObject(indexStatus.index(), XContentBuilder.FieldCaseConversion.NONE);

            builder.startObject(Fields.INDEX);
            if (indexStatus.storeSize() != null) {
                builder.field(Fields.PRIMARY_SIZE, indexStatus.primaryStoreSize().toString());
                builder.field(Fields.PRIMARY_SIZE_IN_BYTES, indexStatus.primaryStoreSize().bytes());
                builder.field(Fields.SIZE, indexStatus.storeSize().toString());
                builder.field(Fields.SIZE_IN_BYTES, indexStatus.storeSize().bytes());
            }
            builder.endObject();
            if (indexStatus.translogOperations() != -1) {
                builder.startObject(Fields.TRANSLOG);
                builder.field(Fields.OPERATIONS, indexStatus.translogOperations());
                builder.endObject();
            }

            if (indexStatus.docs() != null) {
                builder.startObject(Fields.DOCS);
                builder.field(Fields.NUM_DOCS, indexStatus.docs().numDocs());
                builder.field(Fields.MAX_DOC, indexStatus.docs().maxDoc());
                builder.field(Fields.DELETED_DOCS, indexStatus.docs().deletedDocs());
                builder.endObject();
            }

            MergeStats mergeStats = indexStatus.mergeStats();
            if (mergeStats != null) {
                mergeStats.toXContent(builder, params);
            }
            RefreshStats refreshStats = indexStatus.refreshStats();
            if (refreshStats != null) {
                refreshStats.toXContent(builder, params);
            }
            FlushStats flushStats = indexStatus.flushStats();
            if (flushStats != null) {
                flushStats.toXContent(builder, params);
            }

            builder.startObject(Fields.SHARDS);
            for (IndexShardStatus indexShardStatus : indexStatus) {
                builder.startArray(Integer.toString(indexShardStatus.shardId().id()));
                for (ShardStatus shardStatus : indexShardStatus) {
                    builder.startObject();

                    builder.startObject(Fields.ROUTING)
                            .field(Fields.STATE, shardStatus.shardRouting().state())
                            .field(Fields.PRIMARY, shardStatus.shardRouting().primary())
                            .field(Fields.NODE, shardStatus.shardRouting().currentNodeId())
                            .field(Fields.RELOCATING_NODE, shardStatus.shardRouting().relocatingNodeId())
                            .field(Fields.SHARD, shardStatus.shardRouting().shardId().id())
                            .field(Fields.INDEX, shardStatus.shardRouting().shardId().index().name())
                            .endObject();

                    builder.field(Fields.STATE, shardStatus.state());
                    if (shardStatus.storeSize() != null) {
                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.SIZE, shardStatus.storeSize().toString());
                        builder.field(Fields.SIZE_IN_BYTES, shardStatus.storeSize().bytes());
                        builder.endObject();
                    }
                    if (shardStatus.translogId() != -1) {
                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.ID, shardStatus.translogId());
                        builder.field(Fields.OPERATIONS, shardStatus.translogOperations());
                        builder.endObject();
                    }

                    if (shardStatus.docs() != null) {
                        builder.startObject(Fields.DOCS);
                        builder.field(Fields.NUM_DOCS, shardStatus.docs().numDocs());
                        builder.field(Fields.MAX_DOC, shardStatus.docs().maxDoc());
                        builder.field(Fields.DELETED_DOCS, shardStatus.docs().deletedDocs());
                        builder.endObject();
                    }

                    mergeStats = shardStatus.mergeStats();
                    if (mergeStats != null) {
                        mergeStats.toXContent(builder, params);
                    }

                    refreshStats = shardStatus.refreshStats();
                    if (refreshStats != null) {
                        refreshStats.toXContent(builder, params);
                    }
                    flushStats = shardStatus.flushStats();
                    if (flushStats != null) {
                        flushStats.toXContent(builder, params);
                    }

                    if (shardStatus.peerRecoveryStatus() != null) {
                        PeerRecoveryStatus peerRecoveryStatus = shardStatus.peerRecoveryStatus();
                        builder.startObject(Fields.PEER_RECOVERY);
                        builder.field(Fields.STAGE, peerRecoveryStatus.stage());
                        builder.field(Fields.START_TIME_IN_MILLIS, peerRecoveryStatus.startTime());
                        builder.field(Fields.TIME, peerRecoveryStatus.time());
                        builder.field(Fields.TIME_IN_MILLIS, peerRecoveryStatus.time().millis());

                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.PROGRESS, peerRecoveryStatus.indexRecoveryProgress());
                        builder.field(Fields.SIZE, peerRecoveryStatus.indexSize());
                        builder.field(Fields.SIZE_IN_BYTES, peerRecoveryStatus.indexSize().bytes());
                        builder.field(Fields.REUSED_SIZE, peerRecoveryStatus.reusedIndexSize());
                        builder.field(Fields.REUSED_SIZE_IN_BYTES, peerRecoveryStatus.reusedIndexSize().bytes());
                        builder.field(Fields.EXPECTED_RECOVERED_SIZE, peerRecoveryStatus.expectedRecoveredIndexSize());
                        builder.field(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, peerRecoveryStatus.expectedRecoveredIndexSize().bytes());
                        builder.field(Fields.RECOVERED_SIZE, peerRecoveryStatus.recoveredIndexSize());
                        builder.field(Fields.RECOVERED_SIZE_IN_BYTES, peerRecoveryStatus.recoveredIndexSize().bytes());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.RECOVERED, peerRecoveryStatus.recoveredTranslogOperations());
                        builder.endObject();

                        builder.endObject();
                    }

                    if (shardStatus.gatewayRecoveryStatus() != null) {
                        GatewayRecoveryStatus gatewayRecoveryStatus = shardStatus.gatewayRecoveryStatus();
                        builder.startObject(Fields.GATEWAY_RECOVERY);
                        builder.field(Fields.STAGE, gatewayRecoveryStatus.stage());
                        builder.field(Fields.START_TIME_IN_MILLIS, gatewayRecoveryStatus.startTime());
                        builder.field(Fields.TIME, gatewayRecoveryStatus.time());
                        builder.field(Fields.TIME_IN_MILLIS, gatewayRecoveryStatus.time().millis());

                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.PROGRESS, gatewayRecoveryStatus.indexRecoveryProgress());
                        builder.field(Fields.SIZE, gatewayRecoveryStatus.indexSize());
                        builder.field(Fields.SIZE_IN_BYTES, gatewayRecoveryStatus.indexSize().bytes());
                        builder.field(Fields.REUSED_SIZE, gatewayRecoveryStatus.reusedIndexSize());
                        builder.field(Fields.REUSED_SIZE_IN_BYTES, gatewayRecoveryStatus.reusedIndexSize().bytes());
                        builder.field(Fields.EXPECTED_RECOVERED_SIZE, gatewayRecoveryStatus.expectedRecoveredIndexSize());
                        builder.field(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, gatewayRecoveryStatus.expectedRecoveredIndexSize().bytes());
                        builder.field(Fields.RECOVERED_SIZE, gatewayRecoveryStatus.recoveredIndexSize());
                        builder.field(Fields.RECOVERED_SIZE_IN_BYTES, gatewayRecoveryStatus.recoveredIndexSize().bytes());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.RECOVERED, gatewayRecoveryStatus.recoveredTranslogOperations());
                        builder.endObject();

                        builder.endObject();
                    }

                    if (shardStatus.gatewaySnapshotStatus() != null) {
                        GatewaySnapshotStatus gatewaySnapshotStatus = shardStatus.gatewaySnapshotStatus();
                        builder.startObject(Fields.GATEWAY_SNAPSHOT);
                        builder.field(Fields.STAGE, gatewaySnapshotStatus.stage());
                        builder.field(Fields.START_TIME_IN_MILLIS, gatewaySnapshotStatus.startTime());
                        builder.field(Fields.TIME, gatewaySnapshotStatus.time());
                        builder.field(Fields.TIME_IN_MILLIS, gatewaySnapshotStatus.time().millis());

                        builder.startObject(Fields.INDEX);
                        builder.field(Fields.SIZE, gatewaySnapshotStatus.indexSize());
                        builder.field(Fields.SIZE_IN_BYTES, gatewaySnapshotStatus.indexSize().bytes());
                        builder.endObject();

                        builder.startObject(Fields.TRANSLOG);
                        builder.field(Fields.EXPECTED_OPERATIONS, gatewaySnapshotStatus.expectedNumberOfOperations());
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
