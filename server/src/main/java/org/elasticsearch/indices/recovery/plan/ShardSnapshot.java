/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.engine.Engine.HISTORY_UUID_KEY;

class ShardSnapshot {
    private final ShardSnapshotInfo shardSnapshotInfo;
    // Segment file name -> file info
    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final Map<String, String> userData;

    ShardSnapshot(ShardSnapshotInfo shardSnapshotInfo, List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        this.shardSnapshotInfo = shardSnapshotInfo;
        this.snapshotFiles = snapshotFiles.stream()
            .collect(Collectors.toMap(snapshotFile -> snapshotFile.metadata().name(), Function.identity()));
        this.metadataSnapshot = convertToMetadataSnapshot(snapshotFiles);
        this.userData = readUserData(this.snapshotFiles);
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

    ShardSnapshotInfo getShardSnapshotInfo() {
        return shardSnapshotInfo;
    }

    List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFiles(List<StoreFileMetadata> segmentFiles) {
        return segmentFiles.stream()
            .map(storeFileMetadata -> snapshotFiles.get(storeFileMetadata.name()))
            .collect(Collectors.toList());
    }

    String getHistoryUUID() {
        return userData.getOrDefault(HISTORY_UUID_KEY, "__na__");
    }

    static Map<String, String> readUserData(Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
        try {
            SnapshotMetadataDirectory snapshotMetadataDirectory = new SnapshotMetadataDirectory(snapshotFiles);
            return Lucene.readSegmentInfos(snapshotMetadataDirectory).getUserData();
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    static class SnapshotMetadataDirectory extends BaseDirectory {
        private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;

        SnapshotMetadataDirectory(Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
            super(NoLockFactory.INSTANCE);
            this.snapshotFiles = snapshotFiles;
        }

        @Override
        public String[] listAll() {
            return snapshotFiles.keySet().toArray(new String[0]);
        }

        @Override
        public void deleteFile(String name) {
            throw unsupportedOperation();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return getFile(name).length();
        }

        private BlobStoreIndexShardSnapshot.FileInfo getFile(String name) throws IOException {
            final BlobStoreIndexShardSnapshot.FileInfo fileInfo = snapshotFiles.get(name);
            if (fileInfo == null) {
                throw new FileNotFoundException(name);
            }
            return fileInfo;
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) {
            throw unsupportedOperation();
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
            throw unsupportedOperation();
        }

        @Override
        public void sync(Collection<String> names) {
            throw unsupportedOperation();
        }

        @Override
        public void syncMetaData() {
            throw unsupportedOperation();
        }

        @Override
        public void rename(String source, String dest) {
            throw unsupportedOperation();
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            final BlobStoreIndexShardSnapshot.FileInfo file = getFile(name);
            final StoreFileMetadata metadata = file.metadata();
            if (metadata.hashEqualsContents() == false) {
                throw new IOException("Unable to read " + name);
            }

            BytesRef content = metadata.hash();
            return new ByteArrayIndexInput(name, content.bytes, content.offset, content.length);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Set<String> getPendingDeletions() {
            return Collections.emptySet();
        }

        private UnsupportedOperationException unsupportedOperation() {
            return new UnsupportedOperationException();
        }
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
