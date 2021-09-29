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
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.SNAPSHOT_RECOVERIES_SUPPORTED_VERSION;

public class ShardSnapshotsService {
    private final Logger logger = LogManager.getLogger(ShardSnapshotsService.class);

    private final Client client;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public ShardSnapshotsService(Client client,
                                 RepositoriesService repositoriesService,
                                 ThreadPool threadPool,
                                 ClusterService clusterService) {
        this.client = client;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public void fetchLatestSnapshotsForShard(ShardId shardId, ActionListener<Optional<ShardSnapshot>> listener) {
        assert shardId != null : "SharId was null but a value was expected";

        final RepositoriesMetadata currentReposMetadata = clusterService.state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

        List<String> repositories = currentReposMetadata.repositories()
            .stream()
            .filter(repositoryMetadata -> BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.get(repositoryMetadata.settings()))
            .map(RepositoryMetadata::name)
            .collect(Collectors.toList());

        if (repositories.isEmpty() || masterSupportsFetchingLatestSnapshots() == false) {
            logger.debug("Unable to use snapshots during peer recovery use_for_peer_recovery_repositories=[{}]," +
                " masterSupportsFetchingLatestSnapshots=[{}]", repositories, masterSupportsFetchingLatestSnapshots());
            listener.onResponse(Optional.empty());
            return;
        }

        logger.debug("Searching for peer recovery compatible snapshots in [{}]", repositories);

        GetShardSnapshotRequest request = GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, repositories);
        client.execute(GetShardSnapshotAction.INSTANCE,
            request,
            new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, listener.map(this::fetchSnapshotFiles), false)
        );
    }

    private Optional<ShardSnapshot> fetchSnapshotFiles(GetShardSnapshotResponse shardSnapshotResponse) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);

        final Optional<ShardSnapshotInfo> latestShardSnapshotOpt = shardSnapshotResponse.getLatestShardSnapshot();
        if (latestShardSnapshotOpt.isEmpty()) {
            return Optional.empty();
        }

        final ShardSnapshotInfo latestShardSnapshot = latestShardSnapshotOpt.get();
        try {
            final Snapshot snapshot = latestShardSnapshot.getSnapshot();

            final Repository repository = repositoriesService.repository(snapshot.getRepository());
            if (repository instanceof BlobStoreRepository == false) {
                return Optional.empty();
            }

            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            BlobContainer blobContainer = blobStoreRepository.shardContainer(latestShardSnapshot.getIndexId(),
                latestShardSnapshot.getShardId().getId());
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot =
                blobStoreRepository.loadShardSnapshot(blobContainer, snapshot.getSnapshotId());

            return Optional.of(new ShardSnapshot(latestShardSnapshot, blobStoreIndexShardSnapshot.indexFiles()));
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Unable to fetch shard snapshot files for {}", latestShardSnapshot), e);
            return Optional.empty();
        }
    }

    protected boolean masterSupportsFetchingLatestSnapshots() {
        return clusterService.state().nodes().getMinNodeVersion().onOrAfter(SNAPSHOT_RECOVERIES_SUPPORTED_VERSION);
    }
}
