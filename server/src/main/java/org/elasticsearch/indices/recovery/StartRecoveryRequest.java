/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/**
 * Represents a request for starting a peer recovery.
 */
public class StartRecoveryRequest extends AbstractTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final String targetAllocationId;
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final long clusterStateVersion;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final boolean primaryRelocation;
    private final long startingSeqNo;
    private final boolean canDownloadSnapshotFiles;

    public StartRecoveryRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        targetAllocationId = in.readString();
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            clusterStateVersion = in.readVLong();
        } else {
            clusterStateVersion = 0L; // bwc: do not wait for cluster state to be applied
        }
        metadataSnapshot = Store.MetadataSnapshot.readFrom(in);
        primaryRelocation = in.readBoolean();
        startingSeqNo = in.readLong();
        canDownloadSnapshotFiles = in.readBoolean();
    }

    /**
     * Construct a request for starting a peer recovery.
     *
     * @param shardId                  the shard ID to recover
     * @param targetAllocationId       the allocation id of the target shard
     * @param sourceNode               the source node to remover from
     * @param targetNode               the target node to recover to
     * @param clusterStateVersion      the cluster state version which initiated the recovery
     * @param metadataSnapshot         the Lucene metadata
     * @param primaryRelocation        whether or not the recovery is a primary relocation
     * @param recoveryId               the recovery ID
     * @param startingSeqNo            the starting sequence number
     * @param canDownloadSnapshotFiles flag that indicates if the snapshot files can be downloaded
     */
    public StartRecoveryRequest(
        final ShardId shardId,
        final String targetAllocationId,
        final DiscoveryNode sourceNode,
        final DiscoveryNode targetNode,
        final long clusterStateVersion,
        final Store.MetadataSnapshot metadataSnapshot,
        final boolean primaryRelocation,
        final long recoveryId,
        final long startingSeqNo,
        final boolean canDownloadSnapshotFiles
    ) {
        this.clusterStateVersion = clusterStateVersion;
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetAllocationId = targetAllocationId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.metadataSnapshot = metadataSnapshot;
        this.primaryRelocation = primaryRelocation;
        this.startingSeqNo = startingSeqNo;
        this.canDownloadSnapshotFiles = canDownloadSnapshotFiles;
        assert startingSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || metadataSnapshot.getHistoryUUID() != null
            : "starting seq no is set but not history uuid";
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

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    public boolean isPrimaryRelocation() {
        return primaryRelocation;
    }

    public Store.MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    public long startingSeqNo() {
        return startingSeqNo;
    }

    public boolean canDownloadSnapshotFiles() {
        return canDownloadSnapshotFiles;
    }

    @Override
    public String getDescription() {
        return Strings.format(
            """
                recovery of %s to %s \
                [recoveryId=%d, targetAllocationId=%s, clusterStateVersion=%d, startingSeqNo=%d, \
                primaryRelocation=%s, canDownloadSnapshotFiles=%s]""",
            shardId,
            targetNode.descriptionWithoutAttributes(),
            recoveryId,
            targetAllocationId,
            clusterStateVersion,
            startingSeqNo,
            primaryRelocation,
            canDownloadSnapshotFiles
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeString(targetAllocationId);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            out.writeVLong(clusterStateVersion);
        } // else bwc: just omit it, the receiver doesn't wait for a cluster state anyway
        metadataSnapshot.writeTo(out);
        out.writeBoolean(primaryRelocation);
        out.writeLong(startingSeqNo);
        out.writeBoolean(canDownloadSnapshotFiles);
    }

    @Override
    public String toString() {
        return "StartRecoveryRequest{"
            + "shardId="
            + shardId
            + ", targetNode="
            + targetNode.descriptionWithoutAttributes()
            + ", recoveryId="
            + recoveryId
            + ", targetAllocationId='"
            + targetAllocationId
            + "', clusterStateVersion="
            + clusterStateVersion
            + ", primaryRelocation="
            + primaryRelocation
            + ", startingSeqNo="
            + startingSeqNo
            + ", canDownloadSnapshotFiles="
            + canDownloadSnapshotFiles
            + '}';
    }
}
