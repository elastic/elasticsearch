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
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.List;

public class ShardSnapshotInfo implements Writeable {
    private final IndexId indexId;
    private final SnapshotInfo snapshotInfo;
    private final ShardId shardId;
    private final String indexMetadataIdentifier;
    private final SnapshotFiles snapshotFiles;

    public ShardSnapshotInfo(
        IndexId indexId,
        ShardId shardId,
        SnapshotInfo snapshotInfo,
        String indexMetadataIdentifier,
        SnapshotFiles snapshotFiles
    ) {
        assert snapshotInfo.indices().contains(indexId.getName());
        assert snapshotInfo.state() == SnapshotState.SUCCESS;

        this.indexId = indexId;
        this.shardId = shardId;
        this.snapshotInfo = snapshotInfo;
        this.indexMetadataIdentifier = indexMetadataIdentifier;
        this.snapshotFiles = snapshotFiles;
    }

    public ShardSnapshotInfo(StreamInput in) throws IOException {
        this.indexId = new IndexId(in);
        this.snapshotInfo = SnapshotInfo.readFrom(in);
        this.shardId = new ShardId(in);
        this.indexMetadataIdentifier = in.readString();
        this.snapshotFiles = new SnapshotFiles(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexId.writeTo(out);
        snapshotInfo.writeTo(out);
        shardId.writeTo(out);
        out.writeString(indexMetadataIdentifier);
        snapshotFiles.writeTo(out);
    }

    @Nullable
    public String stableShardIdentifier() {
        // It might be null if the shard had in-flight operations and localCheckpoint != maxSeqNo while it was snapshotted
        return snapshotFiles.shardStateIdentifier();
    }

    public String getIndexMetadataIdentifier() {
        return indexMetadataIdentifier;
    }

    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    public List<BlobStoreIndexShardSnapshot.FileInfo> getIndexFiles() {
        return snapshotFiles.indexFiles();
    }
}
