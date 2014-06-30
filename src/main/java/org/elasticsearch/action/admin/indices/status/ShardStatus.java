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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.IndexShardState;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;
import static org.elasticsearch.common.unit.ByteSizeValue.readBytesSizeValue;

/**
 * This class will be removed in future versions
 * Use the recovery API instead
 */
@Deprecated
public class ShardStatus extends BroadcastShardOperationResponse {

    private ShardRouting shardRouting;

    IndexShardState state;

    ByteSizeValue storeSize;

    long translogId = -1;

    long translogOperations = -1;

    DocsStatus docs;

    MergeStats mergeStats;

    RefreshStats refreshStats;

    FlushStats flushStats;

    PeerRecoveryStatus peerRecoveryStatus;

    GatewayRecoveryStatus gatewayRecoveryStatus;

    GatewaySnapshotStatus gatewaySnapshotStatus;

    ShardStatus() {
    }

    ShardStatus(ShardRouting shardRouting) {
        super(shardRouting.index(), shardRouting.id());
        this.shardRouting = shardRouting;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    /**
     * The shard state (index/local state).
     */
    public IndexShardState getState() {
        return state;
    }

    /**
     * The current size of the shard index storage.
     */
    public ByteSizeValue getStoreSize() {
        return storeSize;
    }

    /**
     * The transaction log id.
     */
    public long getTranslogId() {
        return translogId;
    }

    /**
     * The number of transaction operations in the transaction log.
     */
    public long getTranslogOperations() {
        return translogOperations;
    }

    /**
     * Docs level information for the shard index, <tt>null</tt> if not applicable.
     */
    public DocsStatus getDocs() {
        return docs;
    }

    /**
     * Index merge statistics.
     */
    public MergeStats getMergeStats() {
        return this.mergeStats;
    }

    /**
     * Refresh stats.
     */
    public RefreshStats getRefreshStats() {
        return this.refreshStats;
    }

    public FlushStats getFlushStats() {
        return this.flushStats;
    }

    /**
     * Peer recovery status (<tt>null</tt> if not applicable). Both real time if an on going recovery
     * is in progress and summary once it is done.
     */
    public PeerRecoveryStatus getPeerRecoveryStatus() {
        return peerRecoveryStatus;
    }

    /**
     * Gateway recovery status (<tt>null</tt> if not applicable). Both real time if an on going recovery
     * is in progress adn summary once it is done.
     */
    public GatewayRecoveryStatus getGatewayRecoveryStatus() {
        return gatewayRecoveryStatus;
    }

    /**
     * The current on going snapshot to the gateway or the last one if none is on going.
     */
    public GatewaySnapshotStatus getGatewaySnapshotStatus() {
        return gatewaySnapshotStatus;
    }

    public static ShardStatus readIndexShardStatus(StreamInput in) throws IOException {
        ShardStatus shardStatus = new ShardStatus();
        shardStatus.readFrom(in);
        return shardStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeByte(state.id());
        if (storeSize == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            storeSize.writeTo(out);
        }
        out.writeLong(translogId);
        out.writeLong(translogOperations);
        if (docs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(docs.getNumDocs());
            out.writeLong(docs.getMaxDoc());
            out.writeLong(docs.getDeletedDocs());
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

        if (mergeStats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            mergeStats.writeTo(out);
        }
        if (refreshStats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            refreshStats.writeTo(out);
        }
        if (flushStats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            flushStats.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        state = IndexShardState.fromId(in.readByte());
        if (in.readBoolean()) {
            storeSize = readBytesSizeValue(in);
        }
        translogId = in.readLong();
        translogOperations = in.readLong();
        if (in.readBoolean()) {
            docs = new DocsStatus();
            docs.numDocs = in.readLong();
            docs.maxDoc = in.readLong();
            docs.deletedDocs = in.readLong();
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

        if (in.readBoolean()) {
            mergeStats = MergeStats.readMergeStats(in);
        }
        if (in.readBoolean()) {
            refreshStats = RefreshStats.readRefreshStats(in);
        }
        if (in.readBoolean()) {
            flushStats = FlushStats.readFlushStats(in);
        }
    }
}
