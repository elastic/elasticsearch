/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

class ShardRecoveryPlan {
    /**
     * The expected shard state after the recovery phase 1 is done
     */
    private final Store.MetadataSnapshot shardMetadataSnapshot;
    private final SnapshotFilesToRecover snapshotFilesToRecover;
    private final List<StoreFileMetadata> sourceFilesToRecover;
    private final List<StoreFileMetadata> identicalFiles;

    private final long startingSeqNo;
    private final int translogOps;

    ShardRecoveryPlan(SnapshotFilesToRecover snapshotFilesToRecover,
                      List<StoreFileMetadata> sourceFilesToRecover,
                      List<StoreFileMetadata> identicalFiles,
                      long startingSeqNo,
                      int translogOps,
                      Store.MetadataSnapshot shardMetadataSnapshot) {
        this.snapshotFilesToRecover = snapshotFilesToRecover;
        this.sourceFilesToRecover = sourceFilesToRecover;
        this.identicalFiles = identicalFiles;
        this.shardMetadataSnapshot = shardMetadataSnapshot;

        this.startingSeqNo = startingSeqNo;
        this.translogOps = translogOps;
    }

    List<String> getIdenticalFileNames() {
        return identicalFiles.stream().map(StoreFileMetadata::name).collect(Collectors.toList());
    }

    List<Long> getIdenticalFileSizes() {
        return identicalFiles.stream().map(StoreFileMetadata::length).collect(Collectors.toList());
    }

    List<String> getFilesToRecoverNames() {
        return getFilesToRecover().map(StoreFileMetadata::name)
            .collect(Collectors.toList());
    }

    List<Long> getFilesToRecoverSizes() {
        return getFilesToRecover().map(StoreFileMetadata::length)
            .collect(Collectors.toList());
    }

    private Stream<StoreFileMetadata> getFilesToRecover() {
        return Stream.concat(
            snapshotFilesToRecover.snapshotFiles.stream().map(BlobStoreIndexShardSnapshot.FileInfo::metadata),
            sourceFilesToRecover.stream()
        );
    }

    long getTotalSize() {
        return 0;
    }

    long getExistingSize() {
        return 0;
    }

    long getStartingSeqNo() {
        return startingSeqNo;
    }

    int getTranslogOps() {
        return translogOps;
    }

    List<StoreFileMetadata> getSourceFilesToRecover() {
        return sourceFilesToRecover;
    }

    Store.MetadataSnapshot getMetadataSnapshot() {
        return shardMetadataSnapshot;
    }

    SnapshotFilesToRecover getSnapshotFilesToRecover() {
        return snapshotFilesToRecover;
    }

    public List<StoreFileMetadata> getIdenticalFiles() {
        return identicalFiles;
    }

    static class SnapshotFilesToRecover implements Iterable<BlobStoreIndexShardSnapshot.FileInfo> {
        static final SnapshotFilesToRecover EMPTY = new SnapshotFilesToRecover(null, emptyList());

        final IndexId indexId;
        final List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;

        SnapshotFilesToRecover(IndexId indexId, List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
            this.indexId = indexId;
            this.snapshotFiles = snapshotFiles;
        }

        IndexId getIndexId() {
            return indexId;
        }

        boolean isEmpty() {
            return snapshotFiles.isEmpty();
        }

        @Override
        public Iterator<BlobStoreIndexShardSnapshot.FileInfo> iterator() {
            return snapshotFiles.iterator();
        }
    }
}
