/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;

import java.io.IOException;

public class RecoverySnapshotFileRequest extends RecoveryTransportRequest {
    private final String repository;
    private final IndexId indexId;
    private final BlobStoreIndexShardSnapshot.FileInfo fileInfo;

    public RecoverySnapshotFileRequest(
        long recoveryId,
        long requestSeqNo,
        ShardId shardId,
        String repository,
        IndexId indexId,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo
    ) {
        super(requestSeqNo, recoveryId, shardId);
        this.repository = repository;
        this.indexId = indexId;
        this.fileInfo = fileInfo;
    }

    public RecoverySnapshotFileRequest(StreamInput in) throws IOException {
        super(in);
        this.repository = in.readString();
        this.indexId = new IndexId(in);
        this.fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getTransportVersion().onOrAfter(RecoverySettings.SNAPSHOT_RECOVERIES_SUPPORTED_TRANSPORT_VERSION)
            : "Unexpected serialization version " + out.getTransportVersion();
        super.writeTo(out);
        out.writeString(repository);
        indexId.writeTo(out);
        fileInfo.writeTo(out);
    }

    public String getRepository() {
        return repository;
    }

    public IndexId getIndexId() {
        return indexId;
    }

    public BlobStoreIndexShardSnapshot.FileInfo getFileInfo() {
        return fileInfo;
    }
}
