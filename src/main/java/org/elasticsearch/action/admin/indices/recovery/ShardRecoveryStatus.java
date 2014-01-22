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

import org.elasticsearch.action.admin.indices.status.GatewayRecoveryStatus;
import org.elasticsearch.action.admin.indices.status.GatewaySnapshotStatus;
import org.elasticsearch.action.admin.indices.status.PeerRecoveryStatus;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;
import static org.elasticsearch.common.unit.ByteSizeValue.readBytesSizeValue;

/**
 *
 */
public class ShardRecoveryStatus extends BroadcastShardOperationResponse implements ToXContent {

    private boolean recovering = false;
    private ShardRouting shardRouting;
    private ByteSizeValue estimatedStoreSize;

    // Only one of the following should be non-null
    private PeerRecoveryStatus peerRecoveryStatus;
    private GatewayRecoveryStatus gatewayRecoveryStatus;
    private GatewaySnapshotStatus gatewaySnapshotStatus;

    ShardRecoveryStatus() {
    }

    ShardRecoveryStatus(ShardRouting shardRouting) {
        super(shardRouting.index(), shardRouting.id());
        this.shardRouting = shardRouting;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public ShardRecoveryStatus peerRecoveryStatus(PeerRecoveryStatus peerRecoveryStatus) {
        this.peerRecoveryStatus = peerRecoveryStatus;
        return this;
    }

    public PeerRecoveryStatus peerRecoveryStatus() {
        return this.peerRecoveryStatus;
    }

    public ShardRecoveryStatus gatewayRecoveryStatus(GatewayRecoveryStatus gatewayRecoveryStatus) {
        this.gatewayRecoveryStatus = gatewayRecoveryStatus;
        return this;
    }

    public ShardRecoveryStatus gatewaySnapshotStatus(GatewaySnapshotStatus gatewaySnapshotStatus) {
        this.gatewaySnapshotStatus = gatewaySnapshotStatus;
        return this;
    }

    public ByteSizeValue getEstimatedStoreSize() {
        return estimatedStoreSize;
    }

    public ShardRecoveryStatus estimatedStoreSize(ByteSizeValue estimatedStoreSize) {
        this.estimatedStoreSize = estimatedStoreSize;
        return this;
    }

    public void recovering(boolean recovering) {
        this.recovering = recovering;
    }

    public boolean recovering() {
        return this.recovering;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        builder.field(Fields.ID, getShardRouting().getId());
        builder.field(Fields.PRIMARY, getShardRouting().primary());
        builder.field(Fields.NODE, getShardRouting().currentNodeId());
        builder.field(Fields.RELOCATING_NODE, getShardRouting().relocatingNodeId());

        // Indicates a restore from a repository snapshot
        if (shardRouting.restoreSource() != null) {
            builder.startObject(Fields.SNAPSHOT_RECOVERY);
            shardRouting.restoreSource().toXContent(builder, params);
            builder.endObject();
        }

        if (peerRecoveryStatus != null) {

            builder.startObject(Fields.PEER_RECOVERY);
            builder.field(Fields.STAGE, peerRecoveryStatus.getStage());
            builder.field(Fields.START_TIME_IN_MILLIS, peerRecoveryStatus.getStartTime());
            builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, peerRecoveryStatus.getTime());

            builder.field(Fields.PROGRESS, peerRecoveryStatus.getIndexRecoveryProgress());
            builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, peerRecoveryStatus.getIndexSize());
            builder.byteSizeField(Fields.REUSED_SIZE_IN_BYTES, Fields.REUSED_SIZE, peerRecoveryStatus.getReusedIndexSize());
            builder.byteSizeField(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, Fields.EXPECTED_RECOVERED_SIZE, peerRecoveryStatus.getExpectedRecoveredIndexSize());
            builder.byteSizeField(Fields.RECOVERED_SIZE_IN_BYTES, Fields.RECOVERED_SIZE, peerRecoveryStatus.getRecoveredIndexSize());

            builder.startObject(Fields.TRANSLOG);
            builder.field(Fields.RECOVERED, peerRecoveryStatus.getRecoveredTranslogOperations());
            builder.endObject();
            builder.endObject();
        }
        if (gatewayRecoveryStatus != null) {

            builder.startObject(Fields.GATEWAY_RECOVERY);
            builder.field(Fields.STAGE, gatewayRecoveryStatus.getStage());
            builder.field(Fields.START_TIME_IN_MILLIS, gatewayRecoveryStatus.getStartTime());
            builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, gatewayRecoveryStatus.getTime());

            builder.field(Fields.PROGRESS, gatewayRecoveryStatus.getIndexRecoveryProgress());
            builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, gatewayRecoveryStatus.getIndexSize());
            builder.byteSizeField(Fields.REUSED_SIZE_IN_BYTES, Fields.REUSED_SIZE, gatewayRecoveryStatus.getReusedIndexSize());
            builder.byteSizeField(Fields.EXPECTED_RECOVERED_SIZE_IN_BYTES, Fields.EXPECTED_RECOVERED_SIZE, gatewayRecoveryStatus.getExpectedRecoveredIndexSize());
            builder.byteSizeField(Fields.RECOVERED_SIZE_IN_BYTES, Fields.RECOVERED_SIZE, gatewayRecoveryStatus.getRecoveredIndexSize());

            builder.startObject(Fields.TRANSLOG);
            builder.field(Fields.RECOVERED, gatewayRecoveryStatus.getRecoveredTranslogOperations());
            builder.endObject();
            builder.endObject();
        }
        if (gatewaySnapshotStatus != null) {

            builder.startObject(Fields.GATEWAY_SNAPSHOT);
            builder.field(Fields.STAGE, gatewaySnapshotStatus.getStage());
            builder.field(Fields.START_TIME_IN_MILLIS, gatewaySnapshotStatus.getStartTime());
            builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, gatewaySnapshotStatus.getTime());

            builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, gatewaySnapshotStatus.getIndexSize());

            builder.startObject(Fields.TRANSLOG);
            builder.field(Fields.EXPECTED_OPERATIONS, gatewaySnapshotStatus.getExpectedNumberOfOperations());
            builder.endObject();
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeBoolean(recovering);
        if (estimatedStoreSize == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            estimatedStoreSize.writeTo(out);
        }

        if (peerRecoveryStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(peerRecoveryStatus.stage.value());
            out.writeVLong(peerRecoveryStatus.startTime);
            out.writeVLong(peerRecoveryStatus.time);
            out.writeVLong(peerRecoveryStatus.indexSize);
            out.writeVLong(peerRecoveryStatus.reusedIndexSize);
            out.writeVLong(peerRecoveryStatus.recoveredIndexSize);
            out.writeVLong(peerRecoveryStatus.recoveredTranslogOperations);
        }

        if (gatewayRecoveryStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(gatewayRecoveryStatus.stage.value());
            out.writeVLong(gatewayRecoveryStatus.startTime);
            out.writeVLong(gatewayRecoveryStatus.time);
            out.writeVLong(gatewayRecoveryStatus.indexSize);
            out.writeVLong(gatewayRecoveryStatus.reusedIndexSize);
            out.writeVLong(gatewayRecoveryStatus.recoveredIndexSize);
            out.writeVLong(gatewayRecoveryStatus.recoveredTranslogOperations);
        }

        if (gatewaySnapshotStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(gatewaySnapshotStatus.stage.value());
            out.writeVLong(gatewaySnapshotStatus.startTime);
            out.writeVLong(gatewaySnapshotStatus.time);
            out.writeVLong(gatewaySnapshotStatus.indexSize);
            out.writeVInt(gatewaySnapshotStatus.getExpectedNumberOfOperations());
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        recovering = in.readBoolean();
        if (in.readBoolean()) {
            estimatedStoreSize = readBytesSizeValue(in);
        }
        if (in.readBoolean()) {
            peerRecoveryStatus = new PeerRecoveryStatus(PeerRecoveryStatus.Stage.fromValue(in.readByte()),
                    in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }
        if (in.readBoolean()) {
            gatewayRecoveryStatus = new GatewayRecoveryStatus(GatewayRecoveryStatus.Stage.fromValue(in.readByte()),
                    in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        if (in.readBoolean()) {
            gatewaySnapshotStatus = new GatewaySnapshotStatus(GatewaySnapshotStatus.Stage.fromValue(in.readByte()),
                    in.readVLong(), in.readVLong(), in.readVLong(), in.readVInt());
        }
    }

    static final class Fields {
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
        static final XContentBuilderString TRANSLOG = new XContentBuilderString("translog");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString RELOCATING_NODE = new XContentBuilderString("relocating_node");
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
        static final XContentBuilderString SNAPSHOT_RECOVERY = new XContentBuilderString("snapshot_recovery");
    }
}
