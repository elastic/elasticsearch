/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CachePreWarmingTests extends ESTestCase {

    public void testCachePreWarming() throws Exception {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "_index",
            Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
                .build()
        );
        final ShardId shardId = new ShardId(indexSettings.getIndex(), randomIntBetween(0, 10));
        final List<Releasable> releasables = new ArrayList<>();

        try (Directory directory = newDirectory()) {
            final IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
                final int nbDocs = scaledRandomIntBetween(0, 1_000);
                final List<String> words = List.of("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog");
                for (int i = 0; i < nbDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new StringField("id", "" + i, Field.Store.YES));
                    String text = String.join(" ", randomSubsetOf(randomIntBetween(1, words.size()), words));
                    doc.add(new TextField("text", text, Field.Store.YES));
                    doc.add(new NumericDocValuesField("rank", i));
                    writer.addDocument(doc);
                }
                if (randomBoolean()) {
                    writer.flush();
                }
                if (randomBoolean()) {
                    writer.forceMerge(1, true);
                }
                final Map<String, String> userData = new HashMap<>(2);
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, "0");
                userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID(random()));
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }

            final ThreadPool threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilder());
            releasables.add(() -> terminate(threadPool));

            final Store store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
            store.incRef();
            releasables.add(store::decRef);
            try {
                final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
                final IndexCommit indexCommit = Lucene.getIndexCommit(segmentInfos, store.directory());

                Path repositoryPath = createTempDir();
                Settings.Builder repositorySettings = Settings.builder().put("location", repositoryPath);
                boolean compress = randomBoolean();
                if (compress) {
                    repositorySettings.put("compress", randomBoolean());
                }
                if (randomBoolean()) {
                    repositorySettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
                }

                final String repositoryName = randomAlphaOfLength(10);
                final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
                    repositoryName,
                    FsRepository.TYPE,
                    repositorySettings.build()
                );

                final BlobStoreRepository repository = new FsRepository(
                    repositoryMetadata,
                    new Environment(
                        Settings.builder()
                            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                            .put(Environment.PATH_REPO_SETTING.getKey(), repositoryPath.toAbsolutePath())
                            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                            .build(),
                        null
                    ),
                    NamedXContentRegistry.EMPTY,
                    BlobStoreTestUtil.mockClusterService(repositoryMetadata)
                ) {

                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we create repo manually on test/main threads
                    }
                };
                repository.start();
                releasables.add(repository::stop);

                final SnapshotId snapshotId = new SnapshotId("_snapshot", UUIDs.randomBase64UUID(random()));
                final IndexId indexId = new IndexId(indexSettings.getIndex().getName(), UUIDs.randomBase64UUID(random()));

                final PlainActionFuture<String> future = PlainActionFuture.newFuture();
                threadPool.generic().submit(() -> {
                    IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
                    repository.snapshotShard(
                        store,
                        null,
                        snapshotId,
                        indexId,
                        indexCommit,
                        null,
                        snapshotStatus,
                        Version.CURRENT,
                        emptyMap(),
                        future
                    );
                    future.actionGet();
                });
                future.actionGet();

                final Path cacheDir = createTempDir();
                final CacheService cacheService = new CacheService(Settings.EMPTY);
                releasables.add(cacheService);
                cacheService.start();

                final List<String> excludedFromCache = randomSubsetOf(Arrays.asList("fdt", "fdx", "nvd", "dvd", "tip", "cfs", "dim"));

                final Settings restoredIndexSettings = Settings.builder()
                    .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                    .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), true)
                    .putList(SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.getKey(), excludedFromCache)
                    .build();

                final BlobContainer blobContainer = repository.shardContainer(indexId, shardId.id());
                final BlobStoreIndexShardSnapshot snapshot = repository.loadShardSnapshot(blobContainer, snapshotId);

                final List<FileInfo> expectedPrewarmedBlobs = snapshot.indexFiles()
                    .stream()
                    .filter(fileInfo -> fileInfo.metadata().hashEqualsContents() == false)
                    .filter(fileInfo -> excludedFromCache.contains(IndexFileNames.getExtension(fileInfo.physicalName())) == false)
                    .collect(Collectors.toList());

                final TrackingFilesBlobContainer filterBlobContainer = new TrackingFilesBlobContainer(blobContainer);
                try (
                    SearchableSnapshotDirectory snapshotDirectory = new SearchableSnapshotDirectory(
                        () -> filterBlobContainer,
                        () -> snapshot,
                        snapshotId,
                        indexId,
                        shardId,
                        restoredIndexSettings,
                        () -> 0L,
                        cacheService,
                        cacheDir,
                        threadPool
                    )
                ) {
                    assertThat(filterBlobContainer.totalFilesRead(), equalTo(0L));
                    assertThat(filterBlobContainer.totalBytesRead(), equalTo(0L));

                    final boolean loaded = snapshotDirectory.loadSnapshot();
                    assertThat("Failed to load snapshot", loaded, is(true));

                    final ExecutorService executor = threadPool.executor(SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME);
                    executor.shutdown();
                    executor.awaitTermination(30L, TimeUnit.SECONDS);

                    assertThat(
                        filterBlobContainer.totalFilesRead(),
                        equalTo(expectedPrewarmedBlobs.stream().mapToLong(FileInfo::numberOfParts).sum())
                    );
                    assertThat(
                        filterBlobContainer.totalBytesRead(),
                        equalTo(expectedPrewarmedBlobs.stream().mapToLong(FileInfo::length).sum())
                    );

                    for (FileInfo expectedPrewarmedBlob : expectedPrewarmedBlobs) {
                        for (int part = 0; part < expectedPrewarmedBlob.numberOfParts(); part++) {
                            String partName = expectedPrewarmedBlob.partName(part);
                            assertThat(filterBlobContainer.totalBytesRead(partName), equalTo(expectedPrewarmedBlob.partBytes(part)));
                        }
                    }
                }
            } finally {
                Releasables.close(releasables);
            }
        }
    }

    private static class TrackingFilesBlobContainer extends FilterBlobContainer {

        private final ConcurrentHashMap<String, Long> files;

        TrackingFilesBlobContainer(BlobContainer delegate) {
            this(delegate, new ConcurrentHashMap<>());
        }

        TrackingFilesBlobContainer(BlobContainer delegate, ConcurrentHashMap<String, Long> files) {
            super(delegate);
            this.files = Objects.requireNonNull(files);
        }

        public long totalFilesRead() {
            return files.size();
        }

        public long totalBytesRead() {
            return files.values().stream().mapToLong(bytesRead -> bytesRead).sum();
        }

        @Nullable
        public Long totalBytesRead(String name) {
            return files.getOrDefault(name, null);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return new FilterInputStream(super.readBlob(blobName, position, length)) {
                long bytesRead = 0L;

                @Override
                public int read() throws IOException {
                    final int result = in.read();
                    if (result == -1) {
                        return result;
                    }
                    bytesRead += 1L;
                    return result;
                }

                @Override
                public int read(byte[] b, int offset, int len) throws IOException {
                    final int result = in.read(b, offset, len);
                    if (result == -1) {
                        return result;
                    }
                    bytesRead += len;
                    return result;
                }

                @Override
                public void close() throws IOException {
                    files.put(blobName, bytesRead);
                    super.close();
                }
            };
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new TrackingFilesBlobContainer(child, this.files);
        }
    }
}
