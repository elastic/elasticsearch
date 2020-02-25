/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
public class SearchableSnapshotDirectory extends BaseDirectory {

    private final BlobStoreIndexShardSnapshot snapshot;
    private final BlobContainer blobContainer;

    public static final IOContext CACHED_READ = new IOContext();
    public static final IOContext NON_CACHED_READ = new IOContext();

    SearchableSnapshotDirectory(final BlobStoreIndexShardSnapshot snapshot, final BlobContainer blobContainer) {
        super(new SingleInstanceLockFactory());
        this.snapshot = Objects.requireNonNull(snapshot);
        this.blobContainer = Objects.requireNonNull(blobContainer);
    }

    private FileInfo fileInfo(final String name) throws FileNotFoundException {
        return snapshot.indexFiles().stream()
            .filter(fileInfo -> fileInfo.physicalName().equals(name))
            .findFirst()
            .orElseThrow(() -> new FileNotFoundException(name));
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return snapshot.indexFiles().stream()
            .map(FileInfo::physicalName)
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        ensureOpen();
        return fileInfo(name).length();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        final boolean cachedRead = context == CACHED_READ;
        return new SearchableSnapshotIndexInput(blobContainer, fileInfo(name), blobContainer.readBlobPreferredLength(cachedRead),
            BufferedIndexInput.BUFFER_SIZE);
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + snapshot.snapshot() + " lockFactory=" + lockFactory;
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        return new UnsupportedOperationException("Searchable snapshot directory does not support this operation");
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

        Directory directory = new SearchableSnapshotDirectory(snapshot, blobContainer);
        if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings.getSettings())) {
            final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
            directory = new CacheDirectory(directory, cache, cacheDir, snapshotId, indexId, shardPath.getShardId(),
                indexSettings.getSettings(), currentTimeNanosSupplier);
        }
        directory = new InMemoryNoOpCommitDirectory(directory);

        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE);

        try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
            final Map<String, String> userData = new HashMap<>();
            indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

            final String translogUUID = Translog.createEmptyTranslog(shardPath.resolveTranslog(),
                Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                shardPath.getShardId(), 0L);

            userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            indexWriter.setLiveCommitData(userData.entrySet());
            indexWriter.commit();
        }

        return directory;
    }

}
