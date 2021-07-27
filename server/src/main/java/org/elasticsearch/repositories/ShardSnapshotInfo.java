/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

public class ShardSnapshotInfo implements Writeable {
    private final IndexId indexId;
    private final Snapshot snapshot;
    private final ShardId shardId;
    private final String indexMetadataIdentifier;
    @Nullable
    private final String shardStateIdentifier;

    public ShardSnapshotInfo(
        IndexId indexId,
        ShardId shardId,
        Snapshot snapshot,
        String indexMetadataIdentifier,
        @Nullable String shardStateIdentifier
    ) {
        this.indexId = indexId;
        this.shardId = shardId;
        this.snapshot = snapshot;
        this.indexMetadataIdentifier = indexMetadataIdentifier;
        this.shardStateIdentifier = shardStateIdentifier;
    }

    public ShardSnapshotInfo(StreamInput in) throws IOException {
        this.indexId = new IndexId(in);
        this.snapshot = new Snapshot(in);
        this.shardId = new ShardId(in);
        this.indexMetadataIdentifier = in.readString();
        this.shardStateIdentifier = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexId.writeTo(out);
        snapshot.writeTo(out);
        shardId.writeTo(out);
        out.writeString(indexMetadataIdentifier);
        out.writeOptionalString(shardStateIdentifier);
    }

    @Nullable
    public String getShardStateIdentifier() {
        // It might be null if the shard had in-flight operations meaning that:
        // localCheckpoint != maxSeqNo || maxSeqNo != indexShard.getLastSyncedGlobalCheckpoint() when the snapshot was taken
        return shardStateIdentifier;
    }

    public String getIndexMetadataIdentifier() {
        return indexMetadataIdentifier;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public String getRepository() {
        return snapshot.getRepository();
    }

    public IndexId getIndexId() {
        return indexId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardSnapshotInfo that = (ShardSnapshotInfo) o;
        return Objects.equals(indexId, that.indexId)
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(indexMetadataIdentifier, that.indexMetadataIdentifier)
            && Objects.equals(shardStateIdentifier, that.shardStateIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexId, snapshot, shardId, indexMetadataIdentifier, shardStateIdentifier);
    }

    @Override
    public String toString() {
        return "ShardSnapshotInfo{"
            + "indexId="
            + indexId
            + ", snapshot="
            + snapshot
            + ", shardId="
            + shardId
            + ", indexMetadataIdentifier='"
            + indexMetadataIdentifier
            + '\''
            + ", shardStateIdentifier='"
            + shardStateIdentifier
            + '\''
            + '}';
    }
}
