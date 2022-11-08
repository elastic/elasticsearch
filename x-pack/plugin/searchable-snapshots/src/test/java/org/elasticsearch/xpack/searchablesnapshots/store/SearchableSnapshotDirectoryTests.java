/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterSeekableByteChannel;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.xpack.searchablesnapshots.store.input.ChecksumBlobContainerIndexInput;
import org.hamcrest.Matcher;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;
import static org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory.getNonNullFileExt;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SearchableSnapshotDirectoryTests extends AbstractSearchableSnapshotsTestCase {

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

    public void testFilesStats() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {

            final Map<String, Long> numFiles = new HashMap<>();
            final Map<String, Long> totalFiles = new HashMap<>();
            final Map<String, Long> minSizes = new HashMap<>();
            final Map<String, Long> maxSizes = new HashMap<>();

            final Predicate<String> ignoredExtensions = fileExtension -> "lock".equals(fileExtension)
                || "si".equals(fileExtension)
                || fileExtension.isEmpty();

            for (String fileName : directory.listAll()) {
                final String extension = getNonNullFileExt(fileName);
                if (ignoredExtensions.test(extension)) {
                    continue;
                }

                // open each file once to force the creation of index input stats
                try (IndexInput input = snapshotDirectory.openInput(fileName, randomIOContext())) {
                    numFiles.compute(extension, (ext, num) -> num == null ? 1L : num + 1L);
                    totalFiles.compute(extension, (ext, total) -> total != null ? total + input.length() : input.length());
                    minSizes.compute(extension, (ext, min) -> Math.min(input.length(), min != null ? min : Long.MAX_VALUE));
                    maxSizes.compute(extension, (ext, max) -> Math.max(input.length(), max != null ? max : Long.MIN_VALUE));
                }
            }

            for (String fileName : snapshotDirectory.listAll()) {
                final String extension = getNonNullFileExt(fileName);
                if (ignoredExtensions.test(extension)) {
                    continue;
                }

                final IndexInputStats inputStats = snapshotDirectory.getStats(fileName);
                assertThat("No index input stats for extension [" + extension + ']', inputStats, notNullValue());
                assertThat(inputStats.getNumFiles(), equalTo(numFiles.get(extension)));
                assertThat(inputStats.getTotalSize(), equalTo(totalFiles.get(extension)));
                assertThat(inputStats.getMinSize(), equalTo(minSizes.get(extension)));
                assertThat(inputStats.getMaxSize(), equalTo(maxSizes.get(extension)));
            }
        });
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

                int available = toIntBytes(indexInput.length() - indexInput.getFilePointer());
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

    public void testChecksumBlobContainerIndexInput() throws Exception {
        testDirectories(
            randomBoolean(),
            false, // no prewarming in this test because we want to ensure that files are accessed on purpose
            (directory, snapshotDirectory) -> {
                for (String fileName : randomSubsetOf(Arrays.asList(snapshotDirectory.listAll()))) {
                    final long checksum;
                    try (IndexInput input = directory.openInput(fileName, Store.READONCE_CHECKSUM)) {
                        checksum = CodecUtil.checksumEntireFile(input);
                    }

                    final long snapshotChecksum;
                    try (IndexInput input = snapshotDirectory.openInput(fileName, Store.READONCE_CHECKSUM)) {
                        snapshotChecksum = CodecUtil.retrieveChecksum(input);
                        assertThat(
                            input,
                            "si".equals(IndexFileNames.getExtension(fileName)) || fileName.startsWith(IndexFileNames.SEGMENTS)
                                ? instanceOf(ByteArrayIndexInput.class)
                                : instanceOf(ChecksumBlobContainerIndexInput.class)
                        );
                    }

                    assertThat(
                        "Expected checksum [" + checksum + "] but got [" + snapshotChecksum + ']',
                        snapshotChecksum,
                        equalTo(checksum)
                    );
                    assertThat(
                        "File [" + fileName + "] should have been read from heap",
                        snapshotDirectory.getStats(fileName),
                        nullValue()
                    );
                }
            }
        );
    }

    public void testMetadataSnapshotsDoesNotAccessFilesOnDisk() throws Exception {
        final ShardId shardId = new ShardId("_name", "_id", 0);
        final IndexSettings indexSettings = newIndexSettings();

        // sometimes load store's MetadataSnapshot using an IndexCommit
        final boolean useIndexCommit = randomBoolean();
        logger.info("--> loading Store.MetadataSnapshot using index commit is [{}]", useIndexCommit);
        final CheckedFunction<Store, Store.MetadataSnapshot, IOException> loader = store -> {
            if (useIndexCommit) {
                return store.getMetadata(Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory()));
            } else {
                return store.getMetadata(null, true);
            }
        };

        testDirectories(
            randomBoolean(),
            false, // no prewarming in this test because we want to ensure that files are accessed on purpose
            ((directory, snapshotDirectory) -> {
                final Store.MetadataSnapshot metadata;
                try (Store store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId))) {
                    metadata = loader.apply(store);
                    assertNotNull(metadata);
                }

                final Store.MetadataSnapshot snapshotMetadata;
                try (Store store = new Store(shardId, indexSettings, snapshotDirectory, new DummyShardLock(shardId))) {
                    assertTrue("No files should have been read yet", snapshotDirectory.getStats().isEmpty());
                    snapshotMetadata = store.getMetadata(null);
                    assertTrue("No files should have been read to compute MetadataSnapshot", snapshotDirectory.getStats().isEmpty());
                    assertNotNull(snapshotMetadata);
                }

                final Store.RecoveryDiff diff = randomBoolean()
                    ? metadata.recoveryDiff(snapshotMetadata)
                    : snapshotMetadata.recoveryDiff(metadata);

                assertThat(
                    "List of different files should be empty but got ["
                        + metadata.fileMetadataMap()
                        + "] and ["
                        + snapshotMetadata.fileMetadataMap()
                        + ']',
                    diff.different.isEmpty(),
                    is(true)
                );
                assertThat(
                    "List of missing files should be empty but got ["
                        + metadata.fileMetadataMap()
                        + "] and ["
                        + snapshotMetadata.fileMetadataMap()
                        + ']',
                    diff.missing.isEmpty(),
                    is(true)
                );
                assertThat(
                    "List of files should be identical ["
                        + metadata.fileMetadataMap()
                        + "] and ["
                        + snapshotMetadata.fileMetadataMap()
                        + ']',
                    diff.identical.size(),
                    equalTo(metadata.size())
                );
                assertThat("Number of files should be identical", snapshotMetadata.size(), equalTo(metadata.size()));

                for (StoreFileMetadata storeFileMetadata : metadata) {
                    final StoreFileMetadata snapshotFileMetadata = snapshotMetadata.get(storeFileMetadata.name());
                    assertTrue(
                        storeFileMetadata + " should be identical but got [" + snapshotFileMetadata + ']',
                        storeFileMetadata.isSame(snapshotFileMetadata)
                    );
                }
            })
        );
    }

    /**
     * This method :
     * - sets up a default {@link Directory} and index random documents
     * - snapshots the directory using a FS repository
     * - creates a {@link SearchableSnapshotDirectory} instance based on the snapshotted files
     * - consumes the default and the searchable snapshot directories using the {@link CheckedBiConsumer}.
     */
    private void testDirectories(final CheckedBiConsumer<Directory, SearchableSnapshotDirectory, Exception> consumer) throws Exception {
        testDirectories(randomBoolean(), randomBoolean(), consumer);
    }

    private void testDirectories(
        final boolean enableCache,
        final boolean prewarmCache,
        final CheckedBiConsumer<Directory, SearchableSnapshotDirectory, Exception> consumer
    ) throws Exception {
        testDirectories(enableCache, prewarmCache, createRecoveryState(randomBoolean()), Settings.EMPTY, consumer);
    }

    private void testDirectories(
        final boolean enableCache,
        final boolean prewarmCache,
        final SearchableSnapshotRecoveryState recoveryState,
        final Settings searchableSnapshotDirectorySettings,
        final CheckedBiConsumer<Directory, SearchableSnapshotDirectory, Exception> consumer
    ) throws Exception {
        final IndexSettings indexSettings = newIndexSettings();
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
                final Map<String, String> userData = Maps.newMapWithExpectedSize(2);
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, "0");
                userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID(random()));
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }

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
                    BlobStoreTestUtil.mockClusterService(repositoryMetadata),
                    MockBigArrays.NON_RECYCLING_INSTANCE,
                    new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
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

                final PlainActionFuture<ShardSnapshotResult> future = PlainActionFuture.newFuture();
                threadPool.generic().submit(() -> {
                    IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
                    repository.snapshotShard(
                        new SnapshotShardContext(
                            store,
                            null,
                            snapshotId,
                            indexId,
                            new Engine.IndexCommitRef(indexCommit, () -> {}),
                            null,
                            snapshotStatus,
                            Version.CURRENT,
                            emptyMap(),
                            randomMillisUpToYear9999(),
                            future
                        )
                    );
                    future.actionGet();
                });
                future.actionGet();

                final BlobContainer blobContainer = repository.shardContainer(indexId, shardId.id());
                final BlobStoreIndexShardSnapshot snapshot = repository.loadShardSnapshot(blobContainer, snapshotId);

                final Path shardDir = randomShardPath(shardId);
                final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
                final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
                final CacheService cacheService = defaultCacheService();
                releasables.add(cacheService);
                cacheService.start();
                final FrozenCacheService frozenCacheService = defaultFrozenCacheService();
                releasables.add(frozenCacheService);

                try (
                    SearchableSnapshotDirectory snapshotDirectory = new SearchableSnapshotDirectory(
                        () -> blobContainer,
                        () -> snapshot,
                        new TestUtils.NoopBlobStoreCacheService(),
                        "_repo",
                        snapshotId,
                        indexId,
                        shardId,
                        Settings.builder()
                            .put(searchableSnapshotDirectorySettings)
                            .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), enableCache)
                            .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), prewarmCache)
                            .build(),
                        () -> 0L,
                        cacheService,
                        cacheDir,
                        shardPath,
                        threadPool,
                        frozenCacheService
                    )
                ) {
                    final PlainActionFuture<Void> f = PlainActionFuture.newFuture();
                    final boolean loaded = snapshotDirectory.loadSnapshot(recoveryState, f);
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
                    }
                    assertThat("Failed to load snapshot", loaded, is(true));
                    assertThat("Snapshot should be loaded", snapshotDirectory.snapshot(), sameInstance(snapshot));
                    assertThat("BlobContainer should be loaded", snapshotDirectory.blobContainer(), sameInstance(blobContainer));

                    consumer.accept(directory, snapshotDirectory);
                }
            } finally {
                Releasables.close(releasables);
            }
        } finally {
            assertThreadPoolNotBusy(threadPool);
        }
    }

    private void testIndexInputs(final CheckedBiConsumer<IndexInput, IndexInput, Exception> consumer) throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            for (String fileName : randomSubsetOf(Arrays.asList(snapshotDirectory.listAll()))) {
                final IOContext context = randomIOContext();
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
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();

            final int nbRandomFiles = randomIntBetween(3, 10);
            final List<BlobStoreIndexShardSnapshot.FileInfo> randomFiles = new ArrayList<>(nbRandomFiles);

            final Path shardSnapshotDir = createTempDir();
            for (int i = 0; i < nbRandomFiles; i++) {
                final String fileName = randomAlphaOfLength(5) + randomFileExtension();
                final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 100_000));
                final byte[] input = bytes.v2();
                final String checksum = bytes.v1();
                final String blobName = randomAlphaOfLength(15);
                Files.write(shardSnapshotDir.resolve(blobName), input, StandardOpenOption.CREATE_NEW);
                randomFiles.add(
                    new BlobStoreIndexShardSnapshot.FileInfo(
                        blobName,
                        new StoreFileMetadata(fileName, input.length, checksum, Version.CURRENT.luceneVersion.toString()),
                        ByteSizeValue.ofBytes(input.length)
                    )
                );
            }

            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("_snapshot", 0L, randomFiles, 0L, 0L, 0, 0L);
            final BlobContainer blobContainer = new FsBlobContainer(
                new FsBlobStore(randomIntBetween(1, 8) * 1024, shardSnapshotDir, true),
                BlobPath.EMPTY,
                shardSnapshotDir
            );

            final SnapshotId snapshotId = new SnapshotId("_name", "_uuid");
            final IndexId indexId = new IndexId("_id", "_uuid");
            final ShardId shardId = new ShardId(new Index("_name", "_id"), 0);

            final Path shardDir = randomShardPath(shardId);
            final ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);
            final Path cacheDir = Files.createDirectories(resolveSnapshotCache(shardDir).resolve(snapshotId.getUUID()));
            final FrozenCacheService frozenCacheService = defaultFrozenCacheService();
            try (
                SearchableSnapshotDirectory directory = new SearchableSnapshotDirectory(
                    () -> blobContainer,
                    () -> snapshot,
                    new TestUtils.NoopBlobStoreCacheService(),
                    "_repo",
                    snapshotId,
                    indexId,
                    shardId,
                    Settings.builder()
                        .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                        // disable prewarming in this test to prevent files to be concurrently cached
                        // while the cache is cleared out and while the test verifies it is empty
                        .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                        .build(),
                    () -> 0L,
                    cacheService,
                    cacheDir,
                    shardPath,
                    threadPool,
                    frozenCacheService
                )
            ) {
                final RecoveryState recoveryState = createRecoveryState(randomBoolean());
                final PlainActionFuture<Void> f = PlainActionFuture.newFuture();
                final boolean loaded = directory.loadSnapshot(recoveryState, f);
                f.get();
                assertThat("Failed to load snapshot", loaded, is(true));
                assertThat("Snapshot should be loaded", directory.snapshot(), sameInstance(snapshot));
                assertThat("BlobContainer should be loaded", directory.blobContainer(), sameInstance(blobContainer));

                final byte[] buffer = new byte[1024];
                for (int i = 0; i < randomIntBetween(10, 50); i++) {
                    final BlobStoreIndexShardSnapshot.FileInfo fileInfo = randomFrom(randomFiles);
                    final int fileLength = toIntBytes(fileInfo.length());

                    try (IndexInput input = directory.openInput(fileInfo.physicalName(), randomIOContext())) {
                        assertThat(input.length(), equalTo((long) fileLength));
                        // we can't just read footer as that is not served from cache
                        final int start = between(0, fileLength - CodecUtil.footerLength() - 1);
                        final int end = between(start + 1, fileLength);

                        input.seek(start);
                        while (input.getFilePointer() < end) {
                            input.readBytes(buffer, 0, toIntBytes(Math.min(buffer.length, end - input.getFilePointer())));
                        }
                    }
                    assertListOfFiles(cacheDir, allOf(greaterThan(0), lessThanOrEqualTo(nbRandomFiles)), greaterThan(0L));
                    if (randomBoolean()) {
                        directory.clearCache(true, true);
                        assertBusy(() -> assertListOfFiles(cacheDir, equalTo(0), equalTo(0L)));
                    }
                }
            } finally {
                frozenCacheService.close();
                assertThreadPoolNotBusy(threadPool);
            }
        }
    }

    public void testRequiresAdditionalSettings() {
        final List<Setting<String>> requiredSettings = List.of(
            SNAPSHOT_REPOSITORY_NAME_SETTING,
            SNAPSHOT_INDEX_NAME_SETTING,
            SNAPSHOT_INDEX_ID_SETTING,
            SNAPSHOT_SNAPSHOT_NAME_SETTING,
            SNAPSHOT_SNAPSHOT_ID_SETTING
        );

        for (int i = 0; i < requiredSettings.size(); i++) {
            final Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
            for (int j = 0; j < requiredSettings.size(); j++) {
                if (i != j) {
                    settings.put(requiredSettings.get(j).getKey(), randomAlphaOfLength(10));
                }
            }
            final IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("test").settings(settings).build(), Settings.EMPTY);
            expectThrows(
                IllegalArgumentException.class,
                () -> SearchableSnapshotDirectory.create(null, null, indexSettings, null, null, null, null, null)
            );
        }
    }

    public void testRecoveryStateIsKeptOpenAfterPreWarmFailures() throws Exception {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FaultyReadsFileSystem disruptFileSystemProvider = new FaultyReadsFileSystem(fileSystem);
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);

        try {
            SearchableSnapshotRecoveryState recoveryState = createRecoveryState(true);
            testDirectories(true, true, recoveryState, Settings.EMPTY, (directory, snapshotDirectory) -> {
                boolean areAllFilesReused = snapshotDirectory.snapshot()
                    .indexFiles()
                    .stream()
                    .allMatch(fileInfo -> fileInfo.metadata().hashEqualsContents());
                assertBusy(() -> {
                    // When the number of indexed documents == 0, the index snapshot only contains the
                    // commit file, meaning that the recovery won't fail in that case.
                    RecoveryState.Stage expectedStage = areAllFilesReused ? RecoveryState.Stage.DONE : RecoveryState.Stage.FINALIZE;
                    assertThat(recoveryState.getStage(), equalTo(expectedStage));
                });
                // All pre-warm tasks failed
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
            });
        } finally {
            PathUtilsForTesting.teardown();
        }
    }

    public void testRecoveryStateIsEmptyWhenTheCacheIsNotPreWarmed() throws Exception {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState(true);
        testDirectories(true, false, recoveryState, Settings.EMPTY, (directory, snapshotDirectory) -> {
            assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
            assertThat(recoveryState.getIndex().totalRecoverFiles(), equalTo(0));
        });
    }

    public void testNonCachedFilesAreExcludedFromRecoveryState() throws Exception {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState(true);

        List<String> allFileExtensions = List.of(
            "fdt",
            "fdx",
            "nvd",
            "dvd",
            "tip",
            "cfs",
            "dim",
            "fnm",
            "dvm",
            "tmd",
            "doc",
            "tim",
            "pos",
            "cfe",
            "fdm",
            "nvm"
        );
        List<String> fileTypesExcludedFromCaching = randomSubsetOf(allFileExtensions);
        Settings settings = Settings.builder()
            .putList(SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.getKey(), fileTypesExcludedFromCaching)
            .build();
        testDirectories(true, true, recoveryState, settings, (directory, snapshotDirectory) -> {
            assertBusy(() -> assertTrue(recoveryState.isPreWarmComplete()));

            assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            for (RecoveryState.FileDetail fileDetail : recoveryState.getIndex().fileDetails()) {
                boolean fileHasExcludedType = fileTypesExcludedFromCaching.stream().anyMatch(type -> fileDetail.name().endsWith(type));
                assertFalse(fileHasExcludedType);
            }
        });
    }

    public void testFilesWithHashEqualsContentsAreMarkedAsReusedOnRecoveryState() throws Exception {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState(true);

        testDirectories(true, true, recoveryState, Settings.EMPTY, (directory, snapshotDirectory) -> {
            assertBusy(() -> assertTrue(recoveryState.isPreWarmComplete()));
            assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

            List<BlobStoreIndexShardSnapshot.FileInfo> filesWithEqualContent = snapshotDirectory.snapshot()
                .indexFiles()
                .stream()
                .filter(f -> f.metadata().hashEqualsContents())
                .collect(Collectors.toList());

            for (BlobStoreIndexShardSnapshot.FileInfo fileWithEqualContent : filesWithEqualContent) {
                RecoveryState.FileDetail fileDetail = recoveryState.getIndex().getFileDetails(fileWithEqualContent.physicalName());
                assertThat(fileDetail, is(notNullValue()));
                assertTrue(fileDetail.reused());
            }
        });
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

    private static IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "_index",
            Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
                .build()
        );
    }

    private static class FaultyReadsFileSystem extends FilterFileSystemProvider {
        FaultyReadsFileSystem(FileSystem inner) {
            super("faulty_fs://", inner);
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
            return new FilterSeekableByteChannel(super.newByteChannel(path, options, attrs)) {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    throw new IOException("IO Failure");
                }
            };
        }
    }
}
