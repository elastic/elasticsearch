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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.IndexStorePlugin.DirectoryFactory;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchableSnapshotDirectoryTests extends ESTestCase {

    public void testListAll() throws Exception {
        testDirectories((directory, snapshotDirectory) ->
            assertThat(snapshotDirectory.listAll(), equalTo(Arrays.stream(directory.listAll())
                .filter(file -> "write.lock".equals(file) == false)
                .filter(file -> file.startsWith("extra") == false)
                .toArray(String[]::new))));
    }

    public void testFileLength() throws Exception {
        testDirectories((directory, snapshotDirectory) ->
            Arrays.stream(directory.listAll())
                .filter(file -> "write.lock".equals(file) == false)
                .filter(file -> file.startsWith("extra") == false)
                .forEach(file -> {
                    try {
                        assertThat("File [" + file + "] length mismatch",
                            snapshotDirectory.fileLength(file), equalTo(directory.fileLength(file)));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }));
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
                        Query query = new TermRangeQuery("rank",
                            BytesRefs.toBytesRef(randomLongBetween(0L, 500L)),
                            BytesRefs.toBytesRef(randomLongBetween(501L, 1000L)),
                            randomBoolean(), randomBoolean());
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

                    String field = randomFrom("id", "text");
                    Terms terms = reader.leaves().get(0).reader().terms(field);
                    Terms snapshotTerms = snapshotReader.leaves().get(0).reader().terms(field);
                    assertThat(snapshotTerms.size(), equalTo(terms.size()));
                    assertThat(snapshotTerms.getDocCount(), equalTo(terms.getDocCount()));
                    assertThat(snapshotTerms.getMin(), equalTo(terms.getMin()));
                    assertThat(snapshotTerms.getMax(), equalTo(terms.getMax()));
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
                    assertThat("File pointers values should be the same before reading a byte",
                        snapshotIndexInput, indexInput, IndexInput::getFilePointer);

                    if (indexInput.getFilePointer() < indexInput.length()) {
                        assertThat("Read byte result should be the same", snapshotIndexInput, indexInput, IndexInput::readByte);
                    } else {
                        expectThrows(EOFException.class, snapshotIndexInput::readByte);
                    }
                    assertThat("File pointers values should be the same after reading a byte",
                        snapshotIndexInput, indexInput, IndexInput::getFilePointer);
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
                assertThat("File pointers values should be the same before reading a byte",
                    snapshotIndexInput, indexInput, IndexInput::getFilePointer);

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

                assertThat("File pointers values should be the same after reading a byte",
                    snapshotIndexInput, indexInput, IndexInput::getFilePointer);
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
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_index", Settings.builder()
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            .put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT)
            .build());
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

            final ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
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
                final RepositoryMetaData repositoryMetaData =
                    new RepositoryMetaData(repositoryName, SearchableSnapshotRepository.TYPE, repositorySettings.build());

                final BlobStoreRepository repository = new FsRepository(
                    repositoryMetaData,
                    new Environment(Settings.builder()
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                        .put(Environment.PATH_REPO_SETTING.getKey(), repositoryPath.toAbsolutePath())
                        .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths()).build(), null),
                    NamedXContentRegistry.EMPTY, BlobStoreTestUtil.mockClusterService(repositoryMetaData));
                repository.start();
                releasables.add(repository::stop);

                final SnapshotId snapshotId = new SnapshotId("_snapshot", UUIDs.randomBase64UUID(random()));
                final IndexId indexId = new IndexId(indexSettings.getIndex().getName(), UUIDs.randomBase64UUID(random()));

                final PlainActionFuture<String> future = PlainActionFuture.newFuture();
                threadPool.generic().submit(() -> {
                    IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
                    repository.snapshotShard(store, null, snapshotId, indexId, indexCommit, snapshotStatus, true, future);
                    future.actionGet();
                });
                future.actionGet();

                final RepositoriesService repositories = mock(RepositoriesService.class);
                when(repositories.repository(eq(repositoryName))).thenReturn(new SearchableSnapshotRepository(repository));

                final IndexSettings tmpIndexSettings = IndexSettingsModule.newIndexSettings("_searchable_snapshot_index",
                    Settings.builder()
                        .put(indexSettings.getSettings())
                        .put(SearchableSnapshotRepository.getIndexSettings(repository, snapshotId, indexId))
                        .build());

                Path tmpDir = createTempDir().resolve(indexId.getId()).resolve(Integer.toString(shardId.id()));
                ShardId tmpShardId = new ShardId(new Index(indexId.getName(), indexId.getId()), shardId.id());
                ShardPath tmpShardPath = new ShardPath(false, tmpDir, tmpDir, tmpShardId);

                final DirectoryFactory factory = SearchableSnapshotRepository.newDirectoryFactory(() -> repositories);
                try (Directory snapshotDirectory = factory.newDirectory(tmpIndexSettings, tmpShardPath)) {
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

    private static <T> void assertThat(String reason, IndexInput actual, IndexInput expected,
                                       CheckedFunction<IndexInput, ? super T, IOException> eval) throws IOException {
        assertThat(reason
                + "\n\t  actual index input: " + actual.toString()
                + "\n\texpected index input: " + expected.toString(), eval.apply(actual), equalTo(eval.apply(expected)));
    }
}
