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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public class ShardRecoveryPlan {
    private final Store.MetadataSnapshot sourceMetadataSnapshot;
    private final SnapshotFilesToRecover snapshotFilesToRecover;
    private final List<StoreFileMetadata> sourceFilesToRecover;
    private final List<StoreFileMetadata> filesPresentInTarget;

    private final long startingSeqNo;
    private final int translogOps;

    public ShardRecoveryPlan(SnapshotFilesToRecover snapshotFilesToRecover,
                             List<StoreFileMetadata> sourceFilesToRecover,
                             List<StoreFileMetadata> filesPresentInTarget,
                             long startingSeqNo,
                             int translogOps,
                             Store.MetadataSnapshot sourceMetadataSnapshot) {
        this.snapshotFilesToRecover = snapshotFilesToRecover;
        this.sourceFilesToRecover = sourceFilesToRecover;
        this.filesPresentInTarget = filesPresentInTarget;
        this.sourceMetadataSnapshot = sourceMetadataSnapshot;

        this.startingSeqNo = startingSeqNo;
        this.translogOps = translogOps;
    }

    public List<StoreFileMetadata> getFilesPresentInTarget() {
        return filesPresentInTarget;
    }

    public List<String> getFilesPresentInTargetNames() {
        return filesPresentInTarget.stream().map(StoreFileMetadata::name).collect(Collectors.toList());
    }

    public List<Long> getFilesPresentInTargetSizes() {
        return filesPresentInTarget.stream().map(StoreFileMetadata::length).collect(Collectors.toList());
    }

    public List<StoreFileMetadata> getSourceFilesToRecover() {
        return sourceFilesToRecover;
    }

    public List<String> getFilesToRecoverNames() {
        return getFilesToRecoverStream().map(StoreFileMetadata::name)
            .collect(Collectors.toList());
    }

    public List<Long> getFilesToRecoverSizes() {
        return getFilesToRecoverStream().map(StoreFileMetadata::length)
            .collect(Collectors.toList());
    }

    public SnapshotFilesToRecover getSnapshotFilesToRecover() {
        return snapshotFilesToRecover;
    }

    public Store.MetadataSnapshot getSourceMetadataSnapshot() {
        return sourceMetadataSnapshot;
    }

    public long getTotalSize() {
        return Stream.concat(getFilesToRecoverStream(), filesPresentInTarget.stream()).mapToLong(StoreFileMetadata::length).sum();
    }

    public long getExistingSize() {
        return filesPresentInTarget.stream().mapToLong(StoreFileMetadata::length).sum();
    }

    public long getStartingSeqNo() {
        return startingSeqNo;
    }

    public int getTranslogOps() {
        return translogOps;
    }

    private Stream<StoreFileMetadata> getFilesToRecoverStream() {
        return Stream.concat(
            snapshotFilesToRecover.snapshotFiles.stream().map(BlobStoreIndexShardSnapshot.FileInfo::metadata),
            sourceFilesToRecover.stream()
        );
    }

    public static class SnapshotFilesToRecover implements Iterable<BlobStoreIndexShardSnapshot.FileInfo> {
        public static final SnapshotFilesToRecover EMPTY = new SnapshotFilesToRecover(null, null, emptyList());

        private final IndexId indexId;
        private final String repository;
        private final List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;

        public SnapshotFilesToRecover(IndexId indexId, String repository, List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
            this.indexId = indexId;
            this.repository = repository;
            this.snapshotFiles = snapshotFiles;
        }

        public IndexId getIndexId() {
            return indexId;
        }

        public String getRepository() {
            return repository;
        }

        public int size() {
            return snapshotFiles.size();
        }

        public boolean isEmpty() {
            return snapshotFiles.isEmpty();
        }

        public List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFiles() {
            return snapshotFiles;
        }

        @Override
        public Iterator<BlobStoreIndexShardSnapshot.FileInfo> iterator() {
            return snapshotFiles.iterator();
        }
    }
}
