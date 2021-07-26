/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.transport.Transports;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class ShardRecoveryIndexPlanner {
    private final SnapshotInfoFetcher snapshotInfoFetcher;
    private final boolean snapshotRecoveriesEnabled;

    public ShardRecoveryIndexPlanner(SnapshotInfoFetcher snapshotInfoFetcher, boolean snapshotRecoveriesEnabled) {
        this.snapshotInfoFetcher = snapshotInfoFetcher;
        this.snapshotRecoveriesEnabled = snapshotRecoveriesEnabled;
    }

    public void getRecoveryPlanForShard(String shardIdentifier,
                                        Store.MetadataSnapshot recoverySourceMetadata,
                                        Store.MetadataSnapshot recoveryTargetMetadata,
                                        long startingSeqNo,
                                        ShardId shardId,
                                        ActionListener<RecoveryIndexPlan> listener) {
        if (snapshotRecoveriesEnabled == false) {
            planWithoutSnapshots(recoverySourceMetadata, recoveryTargetMetadata, startingSeqNo, listener);
        } else {
            snapshotInfoFetcher.fetchAvailableSnapshots(shardId, new ActionListener<>() {
                @Override
                public void onResponse(List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots) {

                    if (availableSnapshots.isEmpty()) {
                        planWithoutSnapshots(recoverySourceMetadata, recoveryTargetMetadata, startingSeqNo, listener);
                    } else {
                        planIncludingSnapshots(shardIdentifier,
                            availableSnapshots,
                            recoverySourceMetadata,
                            recoveryTargetMetadata,
                            startingSeqNo,
                            listener
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    planWithoutSnapshots(recoverySourceMetadata, recoveryTargetMetadata, startingSeqNo, listener);
                }
            });
        }
    }

    private void planWithoutSnapshots(Store.MetadataSnapshot recoverySourceMetadata,
                                      Store.MetadataSnapshot recoveryTargetMetadata,
                                      long startingSeqNo,
                                      ActionListener<RecoveryIndexPlan> listener) {
        final RecoveryIndexPlan recoveryIndexPlan = getRecoveryIndexPlan(recoverySourceMetadata, recoveryTargetMetadata, startingSeqNo);

        listener.onResponse(recoveryIndexPlan);
    }

    private RecoveryIndexPlan getRecoveryIndexPlan(Store.MetadataSnapshot recoverySourceMetadata,
                                                   Store.MetadataSnapshot recoveryTargetMetadata,
                                                   long startingSeqNo) {
        // Generate a "diff" of all the identical, different, and missing
        // segment files on the target node, using the existing files on
        // the source node
        final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(recoveryTargetMetadata);
        List<StoreFileMetadata> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
        phase1Files.addAll(diff.different);
        phase1Files.addAll(diff.missing);
        return new RecoveryIndexPlan(SnapshotFilesToRecover.EMPTY, diff.identical, phase1Files, startingSeqNo, recoverySourceMetadata);
    }

    private void planIncludingSnapshots(String shardIdentifier,
                                        List<SnapshotInfoFetcher.ShardSnapshotData> availableSnapshots,
                                        Store.MetadataSnapshot recoverySourceMetadata,
                                        Store.MetadataSnapshot recoveryTargetMetadata,
                                        long startingSeqNo,
                                        ActionListener<RecoveryIndexPlan> listener) {
        assert Transports.assertNotTransportThread("Expected to run out of transport thread");

        // TODO: Filter out incompatible snapshots
        //       Check IndexMetadata

        final Store.RecoveryDiff targetDiff = recoverySourceMetadata.recoveryDiff(recoveryTargetMetadata);
        List<StoreFileMetadata> phase1Files = new ArrayList<>(targetDiff.different.size() + targetDiff.missing.size());
        phase1Files.addAll(targetDiff.different);
        phase1Files.addAll(targetDiff.missing);

        // First try to find a logically equivalent snapshot
        final List<SnapshotInfoFetcher.ShardSnapshotData> stableSnapshots = availableSnapshots.stream()
            .filter(shardSnapshotInfo -> shardSnapshotInfo.getShardStateIdentifier() != null)
            .filter(shardSnapshotInfo -> shardSnapshotInfo.getShardStateIdentifier().equals(shardIdentifier))
            .collect(Collectors.toList());

        RecoveryIndexPlan recoveryIndexPlan = null;
        for (SnapshotInfoFetcher.ShardSnapshotData stableSnapshot : stableSnapshots) {
            Store.RecoveryDiff recoveryDiff = recoverySourceMetadata.recoveryDiff(stableSnapshot.getMetadataSnapshot());

            // TODO: Handle primary failovers
            if (recoveryDiff.different.isEmpty() && recoveryDiff.missing.isEmpty()) {
                // Send only phase1 files
                List<BlobStoreIndexShardSnapshot.FileInfo> snap = stableSnapshot.getSnapshotFiles(phase1Files);
                final SnapshotFilesToRecover snapshotFilesToRecover = new SnapshotFilesToRecover(stableSnapshot.getIndexId(), snap);
                recoveryIndexPlan =
                    new RecoveryIndexPlan(snapshotFilesToRecover, emptyList(), emptyList(), startingSeqNo, recoverySourceMetadata);
            }
        }

        if (recoveryIndexPlan != null) {
            listener.onResponse(recoveryIndexPlan);
            return;
        }

        // TODO: try to recover from snapshot + replay operations if possible
        // TODO: Check FORCE_MERGE_UUID, we don't want to discard force merged segments

        final Map<String, StoreFileMetadata> collect = phase1Files.stream()
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
        final Store.MetadataSnapshot storeFileMetadata = new Store.MetadataSnapshot(collect, emptyMap(), 0);

        // If we couldn't find a logically equivalent snapshot, just try to reuse the most files from a snapshot
        int identicalFiles = 0;
        for (SnapshotInfoFetcher.ShardSnapshotData availableSnapshot : availableSnapshots) {
            Store.RecoveryDiff recoveryDiff = storeFileMetadata.recoveryDiff(availableSnapshot.getMetadataSnapshot());
            // TODO: we might want to maximize the data usage instead of file count?
            if (recoveryDiff.identical.size() > identicalFiles) {
                List<BlobStoreIndexShardSnapshot.FileInfo> snap = availableSnapshot.getSnapshotFiles(phase1Files);
                SnapshotFilesToRecover snapshotFilesToRecover = new SnapshotFilesToRecover(availableSnapshot.getIndexId(), snap);

                List<StoreFileMetadata> storeFileMetadata1 = new ArrayList<>(recoveryDiff.different);
                storeFileMetadata1.addAll(recoveryDiff.missing);
                recoveryIndexPlan = new RecoveryIndexPlan(snapshotFilesToRecover, storeFileMetadata1, recoveryDiff.identical,
                    startingSeqNo, recoverySourceMetadata);
                identicalFiles = recoveryDiff.identical.size();
            }
        }

        // We weren't able to find any suitable snapshot, in that case just fallback to baisc peer recovery
        if (recoveryIndexPlan == null) {
            recoveryIndexPlan = new RecoveryIndexPlan(SnapshotFilesToRecover.EMPTY,
                phase1Files, targetDiff.identical, startingSeqNo, recoverySourceMetadata);
        }

        listener.onResponse(recoveryIndexPlan);
    }

    static class RecoveryIndexPlan {
        private final Store.MetadataSnapshot metadataSnapshot;
        private final SnapshotFilesToRecover snapshotFilesToRecover;
        private final List<StoreFileMetadata> identicalFiles;
        private final List<StoreFileMetadata> sourceFiles;

        private final long startingSeqNo;
        private final int translogOps;

        RecoveryIndexPlan(SnapshotFilesToRecover snapshotFilesToRecover,
                          List<StoreFileMetadata> identicalFiles,
                          List<StoreFileMetadata> sourceFiles,
                          long startingSeqNo,
                          Store.MetadataSnapshot metadataSnapshot) {
            this.snapshotFilesToRecover = snapshotFilesToRecover;
            this.identicalFiles = identicalFiles;
            this.sourceFiles = sourceFiles;
            this.metadataSnapshot = metadataSnapshot;

            this.startingSeqNo = startingSeqNo;
            this.translogOps = 0;
        }

        List<String> getIdenticalFileNames() {
            return identicalFiles.stream().map(StoreFileMetadata::name).collect(Collectors.toList());
        }

        List<Long> getIdenticalFileSizes() {
            return identicalFiles.stream().map(StoreFileMetadata::length).collect(Collectors.toList());
        }

        List<String> getMissingFileNames() {
            return Stream.concat(
                snapshotFilesToRecover.snapshotFiles.stream().map(BlobStoreIndexShardSnapshot.FileInfo::metadata),
                sourceFiles.stream()
            ).map(StoreFileMetadata::name)
                .collect(Collectors.toList());
        }

        List<Long> getMissingFileSizes() {
            return Stream.concat(
                snapshotFilesToRecover.snapshotFiles.stream().map(BlobStoreIndexShardSnapshot.FileInfo::metadata),
                sourceFiles.stream()
            ).map(StoreFileMetadata::length)
                .collect(Collectors.toList());
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

        List<StoreFileMetadata> getSourceFiles() {
            return sourceFiles;
        }

        Store.MetadataSnapshot getMetadataSnapshot() {
            return metadataSnapshot;
        }

        SnapshotFilesToRecover getSnapshotFilesToRecover() {
            return snapshotFilesToRecover;
        }

        public List<StoreFileMetadata> getIdenticalFiles() {
            return identicalFiles;
        }
    }

    static class SnapshotFilesToRecover implements Iterable<BlobStoreIndexShardSnapshot.FileInfo> {
        static final SnapshotFilesToRecover EMPTY = new SnapshotFilesToRecover(null, emptyList());

        private final IndexId indexId;
        private final List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;

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
