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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Represents a request for starting a peer recovery.
 */
public class StartRecoveryRequest extends TransportRequest {

    public static final Version WAIT_FOR_RELOCATION_VERSION = Version.V_8_0_0;

    private final long recoveryId;
    private final ShardId shardId;
    private final String targetAllocationId;
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final boolean primaryRelocation;
    private final boolean waitIndefinitelyForRelocation;
    private final long startingSeqNo;

    public StartRecoveryRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        targetAllocationId = in.readString();
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        metadataSnapshot = new Store.MetadataSnapshot(in);
        primaryRelocation = in.readBoolean();
        if (primaryRelocation && in.getVersion().onOrAfter(WAIT_FOR_RELOCATION_VERSION)) {
            waitIndefinitelyForRelocation = in.readBoolean();
        } else {
            waitIndefinitelyForRelocation = false;
        }
        startingSeqNo = in.readLong();
    }

    /**
     * Construct a request for starting a peer recovery.
     *
     * @param shardId                        the shard ID to recover
     * @param targetAllocationId             the allocation id of the target shard
     * @param sourceNode                     the source node to remover from
     * @param targetNode                     the target node to recover to
     * @param metadataSnapshot               the Lucene metadata
     * @param primaryRelocation              whether or not the recovery is a primary relocation
     * @param recoveryId                     the recovery ID
     * @param startingSeqNo                  the starting sequence number
     * @param waitIndefinitelyForRelocation  whether or not to wait for primary relocation handoff indefinitely
     */
    public StartRecoveryRequest(final ShardId shardId,
                                final String targetAllocationId,
                                final DiscoveryNode sourceNode,
                                final DiscoveryNode targetNode,
                                final Store.MetadataSnapshot metadataSnapshot,
                                final boolean primaryRelocation,
                                final long recoveryId,
                                final long startingSeqNo,
                                final boolean waitIndefinitelyForRelocation) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetAllocationId = targetAllocationId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.metadataSnapshot = metadataSnapshot;
        this.primaryRelocation = primaryRelocation;
        this.waitIndefinitelyForRelocation = waitIndefinitelyForRelocation;
        this.startingSeqNo = startingSeqNo;
        assert startingSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || metadataSnapshot.getHistoryUUID() != null :
                        "starting seq no is set but not history uuid";
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String targetAllocationId() {
        return targetAllocationId;
    }

    public DiscoveryNode sourceNode() {
        return sourceNode;
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    public boolean isPrimaryRelocation() {
        return primaryRelocation;
    }

    public boolean waitIndefinitelyForRelocation() {
        return waitIndefinitelyForRelocation;
    }

    public Store.MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    public long startingSeqNo() {
        return startingSeqNo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeString(targetAllocationId);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        metadataSnapshot.writeTo(out);
        out.writeBoolean(primaryRelocation);
        if (primaryRelocation && out.getVersion().onOrAfter(WAIT_FOR_RELOCATION_VERSION)) {
            out.writeBoolean(waitIndefinitelyForRelocation);
        }
        out.writeLong(startingSeqNo);
    }
}
