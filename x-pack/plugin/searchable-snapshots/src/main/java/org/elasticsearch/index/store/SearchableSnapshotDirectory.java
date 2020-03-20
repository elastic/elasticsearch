/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

/**
 * Implementation of {@link Directory} that exposes files from a snapshot as a Lucene directory. Because snapshot are immutable this
 * implementation does not allow modification of the directory files and only supports {@link #listAll()}, {@link #fileLength(String)} and
 * {@link #openInput(String, IOContext)} methods.
 *
 * To create a {@link SearchableSnapshotDirectory} both the list of the snapshot files and a {@link BlobContainer} to read these files must
 * be provided. The definition of the snapshot files are provided using a {@link BlobStoreIndexShardSnapshot} object which contains the name
 * of the snapshot and all the files it contains along with their metadata. Because there is no one-to-one relationship between the original
 * shard files and what it stored in the snapshot the {@link BlobStoreIndexShardSnapshot} is used to map a physical file name as expected by
 * Lucene with the one (or the ones) corresponding blob(s) in the snapshot.
 */
public class SearchableSnapshotDirectory extends BaseSearchableSnapshotDirectory {

    SearchableSnapshotDirectory(final BlobStoreIndexShardSnapshot snapshot, final BlobContainer blobContainer) {
        super(blobContainer, snapshot);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        return new SearchableSnapshotIndexInput(blobContainer, fileInfo(name), context, blobContainer.readBlobPreferredLength(),
            BufferedIndexInput.BUFFER_SIZE);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + snapshot.snapshot() + " lockFactory=" + lockFactory;
    }

    public static Directory create(RepositoriesService repositories,
                                   CacheService cache,
                                   IndexSettings indexSettings,
                                   ShardPath shardPath,
                                   LongSupplier currentTimeNanosSupplier) throws IOException {

        final Repository repository = repositories.repository(SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

        final IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
        final BlobContainer blobContainer = blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id());

        final SnapshotId snapshotId = new SnapshotId(SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()));
        final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);

        final Directory directory;
        if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings.getSettings())) {
            final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
            directory = new CacheDirectory(snapshot, blobContainer, cache, cacheDir, snapshotId, indexId, shardPath.getShardId(),
                currentTimeNanosSupplier);
        } else {
            directory = new SearchableSnapshotDirectory(snapshot, blobContainer);
        }
        return new InMemoryNoOpCommitDirectory(directory);
    }
}
