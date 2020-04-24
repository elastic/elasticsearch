/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
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
import org.hamcrest.Matcher;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class SearchableSnapshotDirectoryTests extends ESTestCase {

    public void testListAll() throws Exception {
        testDirectories(
            (directory, snapshotDirectory) -> assertThat(
                snapshotDirectory.listAll(),
                equalTo(
                    Arrays.stream(directory.listAll())
                        .filter(file -> "write.lock".equals(file) == false)
                        .filter(file -> file.startsWith("extra") == false)
                        .toArray(String[]::new)
                )
            )
        );
    }

    public void testFileLength() throws Exception {
        testDirectories(
            (directory, snapshotDirectory) -> Arrays.stream(directory.listAll())
                .filter(file -> "write.lock".equals(file) == false)
                .filter(file -> file.startsWith("extra") == false)
                .forEach(file -> {
                    try {
                        assertThat(
                            "File [" + file + "] length mismatch",
                            snapshotDirectory.fileLength(file),
                            equalTo(directory.fileLength(file))
                        );
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                })
        );
    }

    public void testIndexSearcher() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final IndexSearcher searcher = newSearcher(reader);

                try (DirectoryReader snapshotReader = DirectoryReader.open(snapshotDirectory)) {
                    final IndexSearcher snapshotSearcher = newSearcher(snapshotReader);
                    {
                        Query query = new MatchAllDocsQuery();
                        assertThat(snapshotSearcher.count(query), equalTo(searcher.count(query)));
                        CheckHits.checkEqual(query, snapshotSearcher.search(query, 10).scoreDocs, searcher.search(query, 10).scoreDocs);
                    }
                    {
                        Query query = new TermQuery(new Term("text", "fox"));
                        assertThat(snapshotSearcher.count(query), equalTo(searcher.count(query)));
                        CheckHits.checkEqual(query, snapshotSearcher.search(query, 10).scoreDocs, searcher.search(query, 10).scoreDocs);
                    }
                    {
                        Query query = new TermInSetQuery("text", List.of(new BytesRef("quick"), new BytesRef("lazy")));
                        assertThat(snapshotSearcher.count(query), equalTo(searcher.count(query)));
                        CheckHits.checkEqual(query, snapshotSearcher.search(query, 10).scoreDocs, searcher.search(query, 10).scoreDocs);
                    }
                    {
                        Query query = new TermRangeQuery(
                            "rank",
                            BytesRefs.toBytesRef(randomLongBetween(0L, 500L)),
                            BytesRefs.toBytesRef(randomLongBetween(501L, 1000L)),
                            randomBoolean(),
                            randomBoolean()
                        );
                        assertThat(snapshotSearcher.count(query), equalTo(searcher.count(query)));
                        CheckHits.checkEqual(query, snapshotSearcher.search(query, 10).scoreDocs, searcher.search(query, 10).scoreDocs);
                    }
                }
            }
        });
    }

    public void testDirectoryReader() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                try (DirectoryReader snapshotReader = DirectoryReader.open(snapshotDirectory)) {
                    assertThat(snapshotReader.leaves(), hasSize(reader.leaves().size()));
                    assertThat(snapshotReader.maxDoc(), equalTo(reader.maxDoc()));
                    assertThat(snapshotReader.getVersion(), equalTo(reader.getVersion()));
                    assertThat(snapshotReader.getIndexCommit().getGeneration(), equalTo(reader.getIndexCommit().getGeneration()));

                    for (int i = 0; i < reader.leaves().size(); i++) {
                        LeafReader leafReader = reader.leaves().get(i).reader();
                        LeafReader snapshotLeafReader = snapshotReader.leaves().get(i).reader();
                        assertThat(snapshotLeafReader.numDocs(), equalTo(leafReader.numDocs()));
                        assertThat(snapshotLeafReader.numDeletedDocs(), equalTo(leafReader.numDeletedDocs()));
                        assertThat(snapshotLeafReader.maxDoc(), equalTo(leafReader.maxDoc()));

                        FieldInfos fieldInfos = leafReader.getFieldInfos();
                        FieldInfos snapshotFieldInfos = snapshotLeafReader.getFieldInfos();
                        assertThat(snapshotFieldInfos.size(), equalTo(fieldInfos.size()));

                        for (int j = 0; j < fieldInfos.size(); j++) {
                            FieldInfo fieldInfo = fieldInfos.fieldInfo(j);
                            FieldInfo snapshotFieldInfo = snapshotFieldInfos.fieldInfo(j);

                            assertThat(snapshotFieldInfo.name, equalTo(fieldInfo.name));
                            assertThat(snapshotFieldInfo.number, equalTo(fieldInfo.number));

                            assertThat(snapshotLeafReader.getDocCount(fieldInfo.name), equalTo(leafReader.getDocCount(fieldInfo.name)));
                            assertThat(snapshotLeafReader.getSumDocFreq(fieldInfo.name), equalTo(leafReader.getSumDocFreq(fieldInfo.name)));

                            assertThat(snapshotFieldInfo.getDocValuesType(), equalTo(fieldInfo.getDocValuesType()));
                            assertThat(snapshotFieldInfo.getDocValuesGen(), equalTo(fieldInfo.getDocValuesGen()));
                            assertThat(snapshotFieldInfo.getPointDimensionCount(), equalTo(fieldInfo.getPointDimensionCount()));
                            assertThat(snapshotFieldInfo.getPointIndexDimensionCount(), equalTo(fieldInfo.getPointIndexDimensionCount()));
                            assertThat(snapshotFieldInfo.getPointNumBytes(), equalTo(fieldInfo.getPointNumBytes()));

                            if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                                Terms terms = leafReader.terms(fieldInfo.name);
                                Terms snapshotTerms = snapshotLeafReader.terms(fieldInfo.name);

                                assertThat(snapshotTerms.size(), equalTo(terms.size()));
                                assertThat(snapshotTerms.getDocCount(), equalTo(terms.getDocCount()));
                                assertThat(snapshotTerms.getMin(), equalTo(terms.getMin()));
                                assertThat(snapshotTerms.getMax(), equalTo(terms.getMax()));
                                assertThat(snapshotTerms.getSumTotalTermFreq(), equalTo(terms.getSumTotalTermFreq()));
                                assertThat(snapshotTerms.getSumDocFreq(), equalTo(terms.getSumDocFreq()));
                            }
                        }
                    }
                }
            }
        });
    }

    public void testReadByte() throws Exception {
        testIndexInputs((indexInput, snapshotIndexInput) -> {
            try {
                for (int i = 0; i < 10; i++) {
                    if (randomBoolean()) {
                        long position = randomLongBetween(0L, indexInput.length());
                        indexInput.seek(position);
                        snapshotIndexInput.seek(position);
                    }
                    assertThat(
                        "File pointers values should be the same before reading a byte",
                        snapshotIndexInput,
                        indexInput,
                        IndexInput::getFilePointer
                    );

                    if (indexInput.getFilePointer() < indexInput.length()) {
                        assertThat("Read byte result should be the same", snapshotIndexInput, indexInput, IndexInput::readByte);
                    } else {
                        expectThrows(EOFException.class, snapshotIndexInput::readByte);
                    }
                    assertThat(
                        "File pointers values should be the same after reading a byte",
                        snapshotIndexInput,
                        indexInput,
                        IndexInput::getFilePointer
                    );
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    public void testReadBytes() throws Exception {
        final byte[] buffer = new byte[8192];
        final byte[] snapshotBuffer = new byte[buffer.length];

        testIndexInputs((indexInput, snapshotIndexInput) -> {
            try {
                if (randomBoolean()) {
                    long position = randomLongBetween(0L, indexInput.length());
                    indexInput.seek(position);
                    snapshotIndexInput.seek(position);
                }
                assertThat(
                    "File pointers values should be the same before reading a byte",
                    snapshotIndexInput,
                    indexInput,
                    IndexInput::getFilePointer
                );

                int available = Math.toIntExact(indexInput.length() - indexInput.getFilePointer());
                if (available == 0) {
                    expectThrows(EOFException.class, () -> snapshotIndexInput.readBytes(snapshotBuffer, 0, snapshotBuffer.length));
                    return;
                }

                int length = randomIntBetween(1, Math.min(available, buffer.length));

                Arrays.fill(buffer, (byte) 0);
                indexInput.readBytes(buffer, 0, length);

                Arrays.fill(snapshotBuffer, (byte) 0);
                snapshotIndexInput.readBytes(snapshotBuffer, 0, length);

                assertThat(
                    "File pointers values should be the same after reading a byte",
                    snapshotIndexInput,
                    indexInput,
                    IndexInput::getFilePointer
                );
                assertArrayEquals(snapshotBuffer, buffer);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    /**
     * This method :
     * - sets up a default {@link Directory} and index random documents
     * - snapshots the directory using a FS repository
     * - creates a {@link SearchableSnapshotDirectory} instance based on the snapshotted files
     * - consumes the default and the searchable snapshot directories using the {@link CheckedBiConsumer}.
     */
    private void testDirectories(final CheckedBiConsumer<Directory, Directory, Exception> consumer) throws Exception {
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
                    repositorySettings.put("base_path", randomAlphaOfLengthBetween(3, 10));
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

                final BlobContainer blobContainer = repository.shardContainer(indexId, shardId.id());
                final BlobStoreIndexShardSnapshot snapshot = repository.loadShardSnapshot(blobContainer, snapshotId);

                final Path cacheDir = createTempDir();
                final CacheService cacheService = new CacheService(Settings.EMPTY);
                releasables.add(cacheService);
                cacheService.start();

                try (
                    SearchableSnapshotDirectory snapshotDirectory = new SearchableSnapshotDirectory(
                        () -> blobContainer,
                        () -> snapshot,
                        snapshotId,
                        indexId,
                        shardId,
                        Settings.builder()
                            .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), randomBoolean())
                            .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), randomBoolean())
                            .build(),
                        () -> 0L,
                        cacheService,
                        cacheDir,
                        threadPool
                    )
                ) {
                    final boolean loaded = snapshotDirectory.loadSnapshot();
                    assertThat("Failed to load snapshot", loaded, is(true));
                    assertThat("Snapshot should be loaded", snapshotDirectory.snapshot(), sameInstance(snapshot));
                    assertThat("BlobContainer should be loaded", snapshotDirectory.blobContainer(), sameInstance(blobContainer));

                    consumer.accept(directory, snapshotDirectory);
                }
            } finally {
                Releasables.close(releasables);
            }
        }
    }

    private void testIndexInputs(final CheckedBiConsumer<IndexInput, IndexInput, Exception> consumer) throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            for (String fileName : randomSubsetOf(Arrays.asList(snapshotDirectory.listAll()))) {
                final IOContext context = newIOContext(random());
                try (IndexInput indexInput = directory.openInput(fileName, context)) {
                    final List<Closeable> closeables = new ArrayList<>();
                    try {
                        IndexInput snapshotIndexInput = snapshotDirectory.openInput(fileName, context);
                        closeables.add(snapshotIndexInput);
                        if (randomBoolean()) {
                            snapshotIndexInput = snapshotIndexInput.clone();
                        }
                        consumer.accept(indexInput, snapshotIndexInput);
                    } finally {
                        IOUtils.close(closeables);
                    }
                }
            }
        });
    }

    public void testClearCache() throws Exception {
        try (CacheService cacheService = new CacheService(Settings.EMPTY)) {
            cacheService.start();

            final int nbRandomFiles = randomIntBetween(3, 10);
            final List<BlobStoreIndexShardSnapshot.FileInfo> randomFiles = new ArrayList<>(nbRandomFiles);

            final Path shardSnapshotDir = createTempDir();
            for (int i = 0; i < nbRandomFiles; i++) {
                final String fileName = "file_" + randomAlphaOfLength(10);
                final byte[] fileContent = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);
                final String blobName = randomAlphaOfLength(15);
                Files.write(shardSnapshotDir.resolve(blobName), fileContent, StandardOpenOption.CREATE_NEW);
                randomFiles.add(
                    new BlobStoreIndexShardSnapshot.FileInfo(
                        blobName,
                        new StoreFileMetadata(fileName, fileContent.length, "_check", Version.CURRENT.luceneVersion),
                        new ByteSizeValue(fileContent.length)
                    )
                );
            }

            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("_snapshot", 0L, randomFiles, 0L, 0L, 0, 0L);
            final BlobContainer blobContainer = new FsBlobContainer(
                new FsBlobStore(Settings.EMPTY, shardSnapshotDir, true),
                BlobPath.cleanPath(),
                shardSnapshotDir
            );

            final SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            final IndexId indexId = new IndexId("_id", "_uuid");
            final ShardId shardId = new ShardId(new Index("_name", "_id"), 0);

            final Path cacheDir = createTempDir();
            final ThreadPool threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilder());
            try (
                SearchableSnapshotDirectory directory = new SearchableSnapshotDirectory(
                    () -> blobContainer,
                    () -> snapshot,
                    snapshotId,
                    indexId,
                    shardId,
                    Settings.builder()
                        .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                        .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), randomBoolean())
                        .build(),
                    () -> 0L,
                    cacheService,
                    cacheDir,
                    threadPool
                )
            ) {

                final boolean loaded = directory.loadSnapshot();
                assertThat("Failed to load snapshot", loaded, is(true));
                assertThat("Snapshot should be loaded", directory.snapshot(), sameInstance(snapshot));
                assertThat("BlobContainer should be loaded", directory.blobContainer(), sameInstance(blobContainer));

                final byte[] buffer = new byte[1024];
                for (int i = 0; i < randomIntBetween(10, 50); i++) {
                    final BlobStoreIndexShardSnapshot.FileInfo fileInfo = randomFrom(randomFiles);
                    final int fileLength = Math.toIntExact(fileInfo.length());

                    try (IndexInput input = directory.openInput(fileInfo.physicalName(), newIOContext(random()))) {
                        assertThat(input.length(), equalTo((long) fileLength));
                        final int start = between(0, fileLength - 1);
                        final int end = between(start + 1, fileLength);

                        input.seek(start);
                        while (input.getFilePointer() < end) {
                            input.readBytes(buffer, 0, Math.toIntExact(Math.min(buffer.length, end - input.getFilePointer())));
                        }
                    }
                    assertListOfFiles(cacheDir, allOf(greaterThan(0), lessThanOrEqualTo(nbRandomFiles)), greaterThan(0L));
                    if (randomBoolean()) {
                        directory.clearCache();
                        assertListOfFiles(cacheDir, equalTo(0), equalTo(0L));
                    }
                }
            } finally {
                terminate(threadPool);
            }
        }
    }

    private static <T> void assertThat(
        String reason,
        IndexInput actual,
        IndexInput expected,
        CheckedFunction<IndexInput, ? super T, IOException> eval
    ) throws IOException {
        assertThat(
            reason + "\n\t  actual index input: " + actual.toString() + "\n\texpected index input: " + expected.toString(),
            eval.apply(actual),
            equalTo(eval.apply(expected))
        );
    }

    private void assertListOfFiles(Path cacheDir, Matcher<Integer> matchNumberOfFiles, Matcher<Long> matchSizeOfFiles) throws IOException {
        final Map<String, Long> files = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(cacheDir)) {
            for (Path file : stream) {
                final String fileName = file.getFileName().toString();
                if (fileName.equals("write.lock") || fileName.startsWith("extra")) {
                    continue;
                }
                try {
                    if (Files.isRegularFile(file)) {
                        final BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
                        files.put(fileName, fileAttributes.size());
                    }
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // ignoring as the cache file might be evicted
                }
            }
        }
        assertThat("Number of files (" + files.size() + ") mismatch, got : " + files.keySet(), files.size(), matchNumberOfFiles);
        assertThat("Sum of file sizes mismatch, got: " + files, files.values().stream().mapToLong(Long::longValue).sum(), matchSizeOfFiles);
    }
}
