/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IndexSnapshotsService {
    private static final Comparator<Tuple<SnapshotId, RepositoryData.SnapshotDetails>> START_TIME_COMPARATOR = Comparator.<
        Tuple<SnapshotId, RepositoryData.SnapshotDetails>>comparingLong(pair -> pair.v2().getStartTimeMillis()).thenComparing(Tuple::v1);

    private final RepositoriesService repositoriesService;

    public IndexSnapshotsService(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public void getLatestSuccessfulSnapshotForShard(
        String repositoryName,
        ShardId shardId,
        ActionListener<Optional<ShardSnapshotInfo>> originalListener
    ) {
        final ActionListener<Optional<ShardSnapshotInfo>> listener = originalListener.delegateResponse(
            (delegate, err) -> {
                delegate.onFailure(
                    new RepositoryException(repositoryName, "Unable to find the latest snapshot for shard [" + shardId + "]", err)
                );
            }
        );

        final Repository repository = getRepository(repositoryName);
        if (repository == null) {
            listener.onFailure(new RepositoryMissingException(repositoryName));
            return;
        }

        final String indexName = shardId.getIndexName();
        StepListener<RepositoryData> repositoryDataStepListener = new StepListener<>();
        StepListener<FetchShardSnapshotContext> snapshotInfoStepListener = new StepListener<>();

        repositoryDataStepListener.whenComplete(repositoryData -> {
            if (repositoryData.hasIndex(indexName) == false) {
                listener.onResponse(Optional.empty());
                return;
            }

            final IndexId indexId = repositoryData.resolveIndexId(indexName);
            final List<SnapshotId> indexSnapshots = repositoryData.getSnapshots(indexId);

            final Optional<SnapshotId> latestSnapshotId = indexSnapshots.stream()
                .map(snapshotId -> Tuple.tuple(snapshotId, repositoryData.getSnapshotDetails(snapshotId)))
                .filter(s -> s.v2().getSnapshotState() != null && s.v2().getSnapshotState() == SnapshotState.SUCCESS)
                .filter(s -> s.v2().getStartTimeMillis() != -1 && s.v2().getEndTimeMillis() != -1)
                .max(START_TIME_COMPARATOR)
                .map(Tuple::v1);

            if (latestSnapshotId.isPresent() == false) {
                // It's possible that some of the backups were taken before 7.14 and they were successful backups, but they don't
                // have the start/end date populated in RepositoryData. We could fetch all the backups and find out if there is
                // a valid candidate, but for simplicity we just consider that we couldn't find any valid snapshot. Existing
                // snapshots start/end timestamps should appear in the RepositoryData eventually.
                listener.onResponse(Optional.empty());
                return;
            }

            final SnapshotId snapshotId = latestSnapshotId.get();
            repository.getSnapshotInfo(
                snapshotId,
                snapshotInfoStepListener.map(
                    snapshotInfo -> new FetchShardSnapshotContext(repository, repositoryData, indexId, shardId, snapshotInfo)
                )
            );
        }, listener::onFailure);

        snapshotInfoStepListener.whenComplete(fetchSnapshotContext -> {
            assert Thread.currentThread().getName().contains('[' + ThreadPool.Names.SNAPSHOT_META + ']')
                : "Expected current thread [" + Thread.currentThread() + "] to be a snapshot meta thread.";
            final SnapshotInfo snapshotInfo = fetchSnapshotContext.getSnapshotInfo();

            if (snapshotInfo == null || snapshotInfo.state() != SnapshotState.SUCCESS) {
                // We couldn't find a valid candidate
                listener.onResponse(Optional.empty());
                return;
            }

            // We fetch BlobStoreIndexShardSnapshots instead of BlobStoreIndexShardSnapshot in order to get the shardStateId that
            // allows us to tell whether or not this shard had in-flight operations while the snapshot was taken.
            final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots = fetchSnapshotContext.getBlobStoreIndexShardSnapshots();
            final String indexMetadataId = fetchSnapshotContext.getIndexMetadataId();

            final Optional<ShardSnapshotInfo> indexShardSnapshotInfo = blobStoreIndexShardSnapshots.snapshots()
                .stream()
                .filter(snapshotFiles -> snapshotFiles.snapshot().equals(snapshotInfo.snapshotId().getName()))
                .findFirst()
                .map(snapshotFiles -> fetchSnapshotContext.createIndexShardSnapshotInfo(indexMetadataId, snapshotFiles));

            listener.onResponse(indexShardSnapshotInfo);
        }, listener::onFailure);

        repository.getRepositoryData(repositoryDataStepListener);
    }

    private Repository getRepository(String repositoryName) {
        final Map<String, Repository> repositories = repositoriesService.getRepositories();
        return repositories.get(repositoryName);
    }

    private static class FetchShardSnapshotContext {
        private final Repository repository;
        private final RepositoryData repositoryData;
        private final IndexId indexId;
        private final ShardId shardId;
        private final SnapshotInfo snapshotInfo;

        FetchShardSnapshotContext(
            Repository repository,
            RepositoryData repositoryData,
            IndexId indexId,
            ShardId shardId,
            SnapshotInfo snapshotInfo
        ) {
            this.repository = repository;
            this.repositoryData = repositoryData;
            this.indexId = indexId;
            this.shardId = shardId;
            this.snapshotInfo = snapshotInfo;
        }

        private String getIndexMetadataId() throws IOException {
            final IndexMetaDataGenerations indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
            String indexMetadataIdentifier = indexMetaDataGenerations.snapshotIndexMetadataIdentifier(snapshotInfo.snapshotId(), indexId);
            if (indexMetadataIdentifier != null) {
                return indexMetadataIdentifier;
            }
            // Fallback to load IndexMetadata from the repository and compute the identifier
            final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId);
            return IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
        }

        private BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots() throws IOException {
            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            final String shardGen = repositoryData.shardGenerations().getShardGen(indexId, shardId.getId());
            return blobStoreRepository.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
        }

        private ShardSnapshotInfo createIndexShardSnapshotInfo(String indexMetadataId, SnapshotFiles snapshotFiles) {
            return new ShardSnapshotInfo(indexId, shardId, snapshotInfo.snapshot(), indexMetadataId, snapshotFiles.shardStateIdentifier());
        }

        SnapshotInfo getSnapshotInfo() {
            return snapshotInfo;
        }
    }
}
