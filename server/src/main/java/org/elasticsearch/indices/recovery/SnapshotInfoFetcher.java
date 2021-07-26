/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class SnapshotInfoFetcher {
    private final Logger logger = LogManager.getLogger(SnapshotInfoFetcher.class);

    private final Client client;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;

    SnapshotInfoFetcher(Client client, RepositoriesService repositoriesService, ThreadPool threadPool) {
        this.client = client;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    protected void fetchAvailableSnapshots(ShardId shardId, ActionListener<List<ShardSnapshotData>> listener) {
        final GetShardSnapshotRequest request = GetShardSnapshotRequest.latestSnapshotInAllRepositories(shardId);
        client.execute(GetShardSnapshotAction.INSTANCE,
            request,
            new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, listener.map(this::fetchSnapshotFiles), false)
        );
    }

    List<ShardSnapshotData> fetchSnapshotFiles(GetShardSnapshotResponse shardSnapshotResponse) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
        if (shardSnapshotResponse.getRepositoryShardSnapshots().isEmpty()) {
          return Collections.emptyList();
        }

        Collection<ShardSnapshotInfo> shardSnapshots = shardSnapshotResponse.getRepositoryShardSnapshots().values();
        List<ShardSnapshotData> shardSnapshotData = new ArrayList<>(shardSnapshots.size());
        for (ShardSnapshotInfo shardSnapshot : shardSnapshots) {
            final List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = getSnapshotFileList(shardSnapshot);
            if (snapshotFiles.isEmpty() == false) {
                shardSnapshotData.add(new ShardSnapshotData(shardSnapshot, snapshotFiles));
            }
        }
        return shardSnapshotData;
    }

    private List<BlobStoreIndexShardSnapshot.FileInfo> getSnapshotFileList(ShardSnapshotInfo shardSnapshotInfo) {
        try {
            final Snapshot snapshot = shardSnapshotInfo.getSnapshot();
            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repositoriesService.repository(snapshot.getRepository());
            BlobContainer blobContainer = blobStoreRepository.shardContainer(shardSnapshotInfo.getIndexId(),
                shardSnapshotInfo.getShardId().getId());
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot =
                blobStoreRepository.loadShardSnapshot(blobContainer, snapshot.getSnapshotId());

            return blobStoreIndexShardSnapshot.indexFiles();
        } catch (Exception e) {
            // TODO: Add a log message here.
            return Collections.emptyList();
        }
    }

    public static class ShardSnapshotData {
        private final ShardSnapshotInfo shardSnapshotInfo;
        // Segment file name -> file info
        private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles;
        private final Store.MetadataSnapshot metadataSnapshot;

        ShardSnapshotData(ShardSnapshotInfo shardSnapshotInfo, List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles) {
            this.shardSnapshotInfo = shardSnapshotInfo;
            this.snapshotFiles = snapshotFiles.stream()
                .collect(Collectors.toMap(snapshotFile -> snapshotFile.metadata().name(), Function.identity()));
            this.metadataSnapshot = convertToMetadataSnapshot(snapshotFiles);
        }

        String getShardStateIdentifier() {
            return shardSnapshotInfo.getShardStateIdentifier();
        }

        Store.MetadataSnapshot getMetadataSnapshot() {
            return metadataSnapshot;
        }

        IndexId getIndexId() {
            return shardSnapshotInfo.getIndexId();
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
}
