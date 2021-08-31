/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class ShardSnapshot {
    private final ShardSnapshotInfo shardSnapshotInfo;
    // Segment file name -> file info
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;
    private final Store.MetadataSnapshot metadataSnapshot;

    ShardSnapshot(ShardSnapshotInfo shardSnapshotInfo, List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        this.shardSnapshotInfo = shardSnapshotInfo;
        this.snapshotFiles = snapshotFiles.stream()
            .collect(Collectors.toMap(snapshotFile -> snapshotFile.metadata().name(), Function.identity()));
        this.metadataSnapshot = convertToMetadataSnapshot(snapshotFiles);
    }

    String getShardStateIdentifier() {
        return shardSnapshotInfo.getShardStateIdentifier();
    }

    String getRepository() {
        return shardSnapshotInfo.getRepository();
    }

    Store.MetadataSnapshot getMetadataSnapshot() {
        return metadataSnapshot;
    }

    IndexId getIndexId() {
        return shardSnapshotInfo.getIndexId();
    }

    long getStartedAt() {
        return shardSnapshotInfo.getStartedAt();
    }

    ShardSnapshotInfo getShardSnapshotInfo() {
        return shardSnapshotInfo;
    }

    List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFiles(List<StoreFileMetadata> segmentFiles) {
        return segmentFiles.stream()
            .map(storeFileMetadata -> snapshotFiles.get(storeFileMetadata.name()))
            .collect(Collectors.toList());
    }

    static Store.MetadataSnapshot convertToMetadataSnapshot(List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        return new Store.MetadataSnapshot(
            snapshotFiles.stream()
                .map(BlobStoreIndexShardSnapshot.FileInfo::metadata)
                .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity())),
            Collections.emptyMap(),
            0
        );
    }
}
