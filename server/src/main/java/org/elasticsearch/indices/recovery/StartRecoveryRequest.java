/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
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

    private final long recoveryId;
    private final ShardId shardId;
    private final String targetAllocationId;
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
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
        metadataSnapshot = Store.MetadataSnapshot.readFrom(in);
        primaryRelocation = in.readBoolean();
        startingSeqNo = in.readLong();
        if (in.getTransportVersion().onOrAfter(RecoverySettings.SNAPSHOT_FILE_DOWNLOAD_THROTTLING_SUPPORTED_TRANSPORT_VERSION)) {
            canDownloadSnapshotFiles = in.readBoolean();
        } else {
            canDownloadSnapshotFiles = true;
        }
    }

    /**
     * Construct a request for starting a peer recovery.
     *
     * @param shardId                  the shard ID to recover
     * @param targetAllocationId       the allocation id of the target shard
     * @param sourceNode               the source node to remover from
     * @param targetNode               the target node to recover to
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
        final Store.MetadataSnapshot metadataSnapshot,
        final boolean primaryRelocation,
        final long recoveryId,
        final long startingSeqNo,
        final boolean canDownloadSnapshotFiles
    ) {
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
                [recoveryId=%d, targetAllocationId=%s, startingSeqNo=%d, primaryRelocation=%s, canDownloadSnapshotFiles=%s]""",
            shardId,
            targetNode.descriptionWithoutAttributes(),
            recoveryId,
            targetAllocationId,
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
        metadataSnapshot.writeTo(out);
        out.writeBoolean(primaryRelocation);
        out.writeLong(startingSeqNo);
        if (out.getTransportVersion().onOrAfter(RecoverySettings.SNAPSHOT_FILE_DOWNLOAD_THROTTLING_SUPPORTED_TRANSPORT_VERSION)) {
            out.writeBoolean(canDownloadSnapshotFiles);
        }
    }
}
