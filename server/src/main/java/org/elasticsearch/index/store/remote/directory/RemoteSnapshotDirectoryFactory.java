/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.IndexShardSnapshot;
import org.elasticsearch.index.store.remote.filecache.FileCache;
import org.elasticsearch.index.store.remote.utils.TransferManager;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public final class RemoteSnapshotDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    public static final String LOCAL_STORE_LOCATION = "RemoteLocalStore";

    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;

    private final FileCache remoteStoreFileCache;

    public RemoteSnapshotDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        FileCache remoteStoreFileCache
    ) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.remoteStoreFileCache = remoteStoreFileCache;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath localShardPath) throws IOException {
        final String repositoryName = IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.get(indexSettings.getSettings());
        final Repository repository = repositoriesService.get().repository(repositoryName);
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        try {
            return createRemoteSnapshotDirectoryFromSnapshot(indexSettings, localShardPath, blobStoreRepository).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Future<RemoteSnapshotDirectory> createRemoteSnapshotDirectoryFromSnapshot(
        IndexSettings indexSettings,
        ShardPath localShardPath,
        BlobStoreRepository blobStoreRepository
    ) throws IOException {
        final BlobPath blobPath = blobStoreRepository.basePath()
            .add("indices")
            .add(IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.get(indexSettings.getSettings()))
            .add(Integer.toString(localShardPath.getShardId().getId()));
        final SnapshotId snapshotId = new SnapshotId(
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.get(indexSettings.getSettings()),
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.get(indexSettings.getSettings())
        );
        Path localStorePath = localShardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(localStorePath));
        // make sure directory is flushed to persistent storage
        localStoreDir.syncMetaData();
        // this trick is needed to bypass assertions in BlobStoreRepository::assertAllowableThreadPools in case of node restart and a remote
        // index restore is invoked
        return threadPool.executor(ThreadPool.Names.SNAPSHOT).submit(() -> {
            final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(blobPath);
            final IndexShardSnapshot indexShardSnapshot = (IndexShardSnapshot) blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);
            assert indexShardSnapshot instanceof BlobStoreIndexShardSnapshot
                : "indexShardSnapshot should be an instance of BlobStoreIndexShardSnapshot";
            final BlobStoreIndexShardSnapshot snapshot = (BlobStoreIndexShardSnapshot) indexShardSnapshot;
            TransferManager transferManager = new TransferManager(blobContainer, remoteStoreFileCache);
            return new RemoteSnapshotDirectory(snapshot, localStoreDir, transferManager);
        });
    }
}
