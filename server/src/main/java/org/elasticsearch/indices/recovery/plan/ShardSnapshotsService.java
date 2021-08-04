/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ShardSnapshotsService {
    private final Logger logger = LogManager.getLogger(ShardSnapshotsService.class);

    private final Client client;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;

    public ShardSnapshotsService(Client client, RepositoriesService repositoriesService, ThreadPool threadPool) {
        this.client = client;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    public void fetchLatestSnapshot(String repository, ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
        if (Strings.isNullOrEmpty(repository)) {
            throw new IllegalArgumentException("A repository should be specified");
        }
        if (shardId == null) {
            throw new IllegalArgumentException("SharId was null but a value was expected");
        }

        GetShardSnapshotRequest request =
            GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, Collections.singletonList(repository));

        client.execute(GetShardSnapshotAction.INSTANCE,
            request,
            new ThreadedActionListener<>(logger,
                threadPool,
                ThreadPool.Names.GENERIC,
                listener.map(response -> fetchSnapshotFiles(repository, response)),
                false
            )
        );
    }

    private Optional<ShardSnapshot> fetchSnapshotFiles(String repository, GetShardSnapshotResponse shardSnapshotResponse) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);

        Optional<ShardSnapshotInfo> shardSnapshotInfoOpt = shardSnapshotResponse.getIndexShardSnapshotInfoForRepository(repository);
        if (shardSnapshotInfoOpt.isEmpty()) {
            return Optional.empty();
        }

        ShardSnapshotInfo shardSnapshotInfo = shardSnapshotInfoOpt.get();
        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = getSnapshotFileList(shardSnapshotInfo);
        return Optional.of(new ShardSnapshot(shardSnapshotInfo, snapshotFiles));
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
            logger.warn(new ParameterizedMessage("Unable to fetch shard snapshot files for {}", shardSnapshotInfo), e);
            throw e;
        }
    }
}
