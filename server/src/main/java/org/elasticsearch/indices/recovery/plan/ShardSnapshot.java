/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
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

import static org.elasticsearch.index.engine.Engine.ES_VERSION;

public class ShardSnapshot {
    private final ShardSnapshotInfo shardSnapshotInfo;
    // Segment file name -> file info
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final Map<String, String> luceneCommitUserData;

    ShardSnapshot(ShardSnapshotInfo shardSnapshotInfo,
                  List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles,
                  Map<String, String> luceneCommitUserData) {
        this.shardSnapshotInfo = shardSnapshotInfo;
        this.snapshotFiles = snapshotFiles.stream()
            .collect(Collectors.toMap(snapshotFile -> snapshotFile.metadata().name(), Function.identity()));
        this.metadataSnapshot = convertToMetadataSnapshot(snapshotFiles);
        this.luceneCommitUserData = luceneCommitUserData;
    }

    public String getShardStateIdentifier() {
        return shardSnapshotInfo.getShardStateIdentifier();
    }

    public boolean isLogicallyEquivalent(@Nullable String shardStateIdentifier) {
        return shardStateIdentifier != null && shardStateIdentifier.equals(shardSnapshotInfo.getShardStateIdentifier());
    }

    public boolean hasDifferentPhysicalFiles(Store.MetadataSnapshot sourceSnapshot) {
        Store.RecoveryDiff recoveryDiff = metadataSnapshot.recoveryDiff(sourceSnapshot);
        return recoveryDiff.different.isEmpty() == false || recoveryDiff.missing.isEmpty() == false;
    }

    public String getRepository() {
        return shardSnapshotInfo.getRepository();
    }

    public Store.MetadataSnapshot getMetadataSnapshot() {
        return metadataSnapshot;
    }

    public IndexId getIndexId() {
        return shardSnapshotInfo.getIndexId();
    }

    public long getStartedAt() {
        return shardSnapshotInfo.getStartedAt();
    }

    public ShardSnapshotInfo getShardSnapshotInfo() {
        return shardSnapshotInfo;
    }

    public Map<String, String> getLuceneCommitUserData() {
        return luceneCommitUserData;
    }

    @Nullable
    public Version getVersion() {
        String version = luceneCommitUserData.get(ES_VERSION);
        return version == null ? null : Version.fromString(version);
    }

    public List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFilesMatching(List<StoreFileMetadata> segmentFiles) {
        return segmentFiles.stream()
            .map(storeFileMetadata -> snapshotFiles.get(storeFileMetadata.name()))
            .collect(Collectors.toList());
    }

    public List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFiles() {
        return List.copyOf(snapshotFiles.values());
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
