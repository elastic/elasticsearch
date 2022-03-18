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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.RecoverySettings.SNAPSHOT_RECOVERIES_SUPPORTED_VERSION;

public class ShardSnapshotsService {
    private final Logger logger = LogManager.getLogger(ShardSnapshotsService.class);

    private final Client client;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    @Inject
    public ShardSnapshotsService(
        Client client,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
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
            .toList();

        if (repositories.isEmpty() || masterSupportsFetchingLatestSnapshots() == false) {
            logger.debug(
                "Unable to use snapshots during peer recovery use_for_peer_recovery_repositories=[{}],"
                    + " masterSupportsFetchingLatestSnapshots=[{}]",
                repositories,
                masterSupportsFetchingLatestSnapshots()
            );
            listener.onResponse(Optional.empty());
            return;
        }

        logger.debug("Searching for peer recovery compatible snapshots in [{}]", repositories);

        GetShardSnapshotRequest request = GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, repositories);
        client.execute(
            GetShardSnapshotAction.INSTANCE,
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
            BlobContainer blobContainer = blobStoreRepository.shardContainer(
                latestShardSnapshot.getIndexId(),
                latestShardSnapshot.getShardId().getId()
            );
            BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot = blobStoreRepository.loadShardSnapshot(
                blobContainer,
                snapshot.getSnapshotId()
            );

            Map<String, StoreFileMetadata> snapshotFiles = blobStoreIndexShardSnapshot.indexFiles()
                .stream()
                .map(BlobStoreIndexShardSnapshot.FileInfo::metadata)
                .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));

            // If the snapshot is taken using a Lucene version that this node cannot read
            // (i.e. the snapshot was taken in a node with version > than this node version)
            // reading the segment commit information could likely fail and we won't be able
            // to recover from a snapshot.
            // This should be a rare edge-case since the allocation deciders won't allow allocating
            // primaries in nodes with older versions.
            //
            // One possible scenario that could lead to having a snapshot taken in a newer node (credits to Henning):
            // 1. We have a primary and a replica.
            // 2. We upgrade one of the nodes.
            // 3. Indexing occurs to both copies.
            // 4. The old version node falls out.
            // 5. A snapshot is done of the new version node.
            // 6. The old version node comes back online.
            // 7. The new version node falls out.
            // 8. The old version node now becomes primary because it has an in-sync copy.
            // 9. We establish a replica on a new version node.
            StoreFileMetadataDirectory directory = new StoreFileMetadataDirectory(snapshotFiles);
            SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(directory);
            Map<String, String> userData = segmentCommitInfos.userData;

            Version commitLuceneVersion = segmentCommitInfos.getCommitLuceneVersion();
            return Optional.of(
                new ShardSnapshot(latestShardSnapshot, blobStoreIndexShardSnapshot.indexFiles(), userData, commitLuceneVersion)
            );
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Unable to fetch shard snapshot files for {}", latestShardSnapshot), e);
            return Optional.empty();
        }
    }

    protected boolean masterSupportsFetchingLatestSnapshots() {
        return clusterService.state().nodes().getMinNodeVersion().onOrAfter(SNAPSHOT_RECOVERIES_SUPPORTED_VERSION);
    }

    private static final class StoreFileMetadataDirectory extends Directory {
        private final Map<String, StoreFileMetadata> files;

        private StoreFileMetadataDirectory(Map<String, StoreFileMetadata> files) {
            this.files = files;
        }

        @Override
        public String[] listAll() {
            return files.keySet().toArray(new String[0]);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            StoreFileMetadata storeFileMetadata = getStoreFileMetadata(name);
            if (storeFileMetadata.hashEqualsContents() == false) {
                throw new IOException("Unable to open " + name);
            }

            final BytesRef data = storeFileMetadata.hash();
            return new ByteArrayIndexInput(name, data.bytes, data.offset, data.length);
        }

        @Override
        public void deleteFile(String name) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public long fileLength(String name) throws IOException {
            final StoreFileMetadata storeFileMetadata = getStoreFileMetadata(name);
            return storeFileMetadata.length();
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public void sync(Collection<String> names) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public void syncMetaData() {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public void rename(String source, String dest) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Set<String> getPendingDeletions() {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        @Override
        public Lock obtainLock(String name) {
            throw new UnsupportedOperationException("this directory is read-only");
        }

        private StoreFileMetadata getStoreFileMetadata(String name) throws IOException {
            final StoreFileMetadata storeFileMetadata = files.get(name);
            if (storeFileMetadata == null) {
                throw new IOException("Unable to find " + name);
            }
            return storeFileMetadata;
        }
    }
}
