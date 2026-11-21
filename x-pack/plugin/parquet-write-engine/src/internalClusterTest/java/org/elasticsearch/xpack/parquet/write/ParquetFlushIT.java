/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.table.CatalogEntry;
import org.elasticsearch.xpack.parquet.write.table.RepositoryCatalog;
import org.elasticsearch.xpack.parquet.write.table.RepositoryFileIO;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Spike S2: a flush merges the local files and uploads them to a real {@code fs} repository as the files of an Iceberg table, and
 * a restart finds what is uploaded and what is not.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetFlushIT extends ESIntegTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String INDEX = "parquet-flush";
    private static final String REPOSITORY = "lake";
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    private void registerRepository() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
    }

    private void createIndex(int shards, Settings.Builder extra) throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .endObject()
            .startObject("count")
            .field("type", "long")
            .endObject()
            .startObject("bytes")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", shards)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", randomFrom("local", "staged"))
                        .put("index.parquet.local_compression", randomFrom("uncompressed", "snappy", "zstd"))
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "@timestamp",
                    BASE_TIMESTAMP + i * 1000L,
                    "host.name",
                    "host-" + (i % 7),
                    "count",
                    i,
                    "bytes",
                    i % 1000
                )
            );
        }
        return bulk.get();
    }

    private IndexShard shard(int shard) {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(shard);
    }

    private long localFilesOf(String index) throws IOException {
        final IndexShard indexShard = internalCluster().getDataNodeInstance(IndicesService.class)
            .indexServiceSafe(resolveIndex(index))
            .getShard(0);
        long files = 0;
        for (String name : indexShard.store().directory().listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    private long localFiles(int shard) throws IOException {
        final Directory directory = shard(shard).store().directory();
        long files = 0;
        for (String name : directory.listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    /** The table of the index, as a reader outside the engine opens it: from the repository directory. */
    private ShardTable table(int shard) throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        final String tablePath = INDEX + "/" + resolveIndex(INDEX).getUUID();
        final FsBlobStore store = new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true);
        return ShardTable.open(
            store,
            new TableLocation(root + "/" + tablePath, BlobPath.EMPTY.add(INDEX).add(resolveIndex(INDEX).getUUID())),
            shard,
            INDEX,
            resolveIndex(INDEX).getUUID(),
            shard(shard).getHistoryUUID(),
            null
        );
    }

    /** The values of {@code count} in the files that the table lists for the shards, each once. */
    private TreeSet<Long> uploadedCounts(int shards) throws IOException {
        final TreeSet<Long> counts = new TreeSet<>();
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        for (int shard = 0; shard < shards; shard++) {
            long expectedFrom = 0;
            for (ShardTable.UploadedFile file : table(shard).uploadedFiles()) {
                assertThat("the files of shard " + shard + " are contiguous", file.fromSeqNo(), equalTo(expectedFrom));
                expectedFrom = file.toSeqNo() + 1;
                final Path path = org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location()));
                final TestParquetBytes.Contents contents = TestParquetBytes.read(java.nio.file.Files.readAllBytes(path));
                assertThat(contents.rows().size(), equalTo((int) file.rows()));
                for (var row : contents.rows()) {
                    assertTrue("count appears once", counts.add(row.getLong("count", 0)));
                }
            }
        }
        return counts;
    }

    private void assertUploaded(int shards, int rows) throws IOException {
        final TreeSet<Long> counts = uploadedCounts(shards);
        assertThat(counts.size(), equalTo(rows));
        assertThat(counts.first(), equalTo(0L));
        assertThat(counts.last(), equalTo(rows - 1L));
    }

    /**
     * Bulks of one shard write their local files in parallel, and flushes (the periodic one and explicit ones) run while they do. A flush
     * must not upload the file of a higher range while a bulk of a lower range is still writing, because it would then delete the lower
     * file as a leftover. After all of it, every row is in the table once, in files with contiguous ranges.
     */
    public void testConcurrentBulksAndFlushesLoseNoRow() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder().put("index.parquet.flush_threshold_size", "1kb"));
        final int writers = 4;
        final int bulksEach = 10;
        final int docsPerBulk = 1_500;
        final AtomicInteger writersDone = new AtomicInteger();
        final AtomicInteger flushes = new AtomicInteger();

        startInParallel(writers + 1, task -> {
            if (task == writers) {
                // flushes until every writer is done
                while (writersDone.get() < writers) {
                    assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
                    flushes.incrementAndGet();
                }
                return;
            }
            try {
                for (int b = 0; b < bulksEach; b++) {
                    final int from = (task * bulksEach + b) * docsPerBulk;
                    assertFalse(bulk(from, from + docsPerBulk).hasFailures());
                }
            } finally {
                writersDone.incrementAndGet();
            }
        });

        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertThat("explicit flushes ran during the writes", flushes.get(), greaterThan(0));
        assertThat(
            indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(),
            equalTo((long) writers * bulksEach * docsPerBulk)
        );
        assertUploaded(1, writers * bulksEach * docsPerBulk);
        assertThat("local files", localFiles(0), equalTo(0L));
    }

    /**
     * A bulk is written column by column as one row group with one page for each column, whatever its size, and the page and the
     * footer have the counts of the rows. The server hands the engine at most 5,000 documents at a time, and the long keywords make a
     * column of each file 2 MB, over the default size of a page (1 MB). The flush has to read the files back and upload every row once.
     * (This guards the structure of the files. It also passed when I wrote the files with the default page limits of parquet-mr.)
     */
    public void testALocalFileHasOneRowGroupAndOnePageForEachColumnWhateverItsSize() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder().put("index.parquet.compaction.enabled", false));
        final int rows = 30_000;
        final String padding = "x".repeat(400);
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < rows; i++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "@timestamp",
                    BASE_TIMESTAMP + i * 1000L,
                    "host.name",
                    "host-" + i + "-" + padding,
                    "count",
                    i,
                    "bytes",
                    i % 1000
                )
            );
        }
        assertFalse(bulk.get().hasFailures());

        // the local files, before a flush merges them
        final Directory directory = shard(0).store().directory();
        final List<LocalParquetFiles.LocalFile> local = LocalParquetFiles.list(directory);
        assertThat(local.size(), greaterThan(1));
        long rowsInFiles = 0;
        for (LocalParquetFiles.LocalFile file : local) {
            try (
                ParquetFileReader reader = ParquetFileReader.open(
                    new IndexInputFile(directory, file.name()),
                    PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
                )
            ) {
                assertThat(reader.getFooter().getBlocks(), hasSize(1));
                final BlockMetaData block = reader.getFooter().getBlocks().get(0);
                rowsInFiles += block.getRowCount();
                for (ColumnChunkMetaData column : block.getColumns()) {
                    final OffsetIndex pages = reader.readOffsetIndex(column);
                    assertThat(file.name() + " pages of " + column.getPath(), pages.getPageCount(), equalTo(1));
                    assertThat(file.name() + " values of " + column.getPath(), column.getValueCount(), equalTo(block.getRowCount()));
                }
            }
        }
        assertThat(rowsInFiles, equalTo((long) rows));

        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertUploaded(1, rows);
        assertThat("local files", localFiles(0), equalTo(0L));
    }

    public void testFlushUploadsTheRowsOfEveryShardAsFilesOfOneTable() throws Exception {
        registerRepository();
        createIndex(2, Settings.builder());

        assertFalse(bulk(0, 12_000).hasFailures());
        assertFalse(bulk(12_000, 30_000).hasFailures());
        final BroadcastResponse response = indicesAdmin().prepareFlush(INDEX).get();
        assertThat(response.getFailedShards(), equalTo(0));

        assertUploaded(2, 30_000);
        for (int shard = 0; shard < 2; shard++) {
            assertThat("local files of shard " + shard, localFiles(shard), equalTo(0L));
            assertThat(table(shard).uploadedFiles().size(), greaterThan(0));
        }
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(30_000L));
        // both shards committed to the same table: its schema has the four columns with the ids that the files carry
        assertThat(table(0).schema().columns(), hasSize(4));
    }

    public void testASecondFlushAddsAFileAndAFlushWithNothingToDoIsHarmless() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder());

        assertFalse(bulk(0, 5_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertThat(indicesAdmin().prepareFlush(INDEX).setForce(true).get().getFailedShards(), equalTo(0));
        assertFalse(bulk(5_000, 8_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertUploaded(1, 8_000);
        assertThat(table(0).uploadedFiles(), hasSize(2));
    }

    public void testThePeriodicFlushUploadsWhenTheLocalFilesReachTheThreshold() throws Exception {
        registerRepository();
        // The local files are compressed (randomly: none, Snappy or ZSTD), and 4,000 of these small documents are a few kilobytes
        // with ZSTD, so the threshold is below that.
        createIndex(1, Settings.builder().put("index.parquet.flush_threshold_size", "1kb"));

        assertFalse(bulk(0, 4_000).hasFailures());

        // The periodic flush is asynchronous. Opening the table in this test creates it when it does not exist, and the store of the
        // helper is read-only, so wait until the engine has created the table before the first look at it.
        final Path firstVersion = repoRoot.resolve(REPOSITORY)
            .resolve(INDEX)
            .resolve(resolveIndex(INDEX).getUUID())
            .resolve("metadata")
            .resolve("v1.metadata.json");
        assertBusy(() -> assertTrue("the engine has created the table", java.nio.file.Files.exists(firstVersion)));
        assertBusy(() -> assertThat(table(0).uploadedFiles().size(), greaterThan(0)));
        assertBusy(() -> assertThat(localFiles(0), equalTo(0L)));
        assertUploaded(1, 4_000);
    }

    public void testARestartKeepsWhatIsUploadedAndWhatIsOnlyLocal() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder());
        assertFalse(bulk(0, 5_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulk(5_000, 7_000).hasFailures());
        assertThat(localFiles(0), greaterThan(0L));

        internalCluster().fullRestart();
        ensureGreen(INDEX);

        // the doc count adds the rows that were uploaded and the rows that are only local, and the sequence continues
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(7_000L));
        assertThat(shard(0).seqNoStats().getMaxSeqNo(), equalTo(6_999L));
        assertFalse(bulk(7_000, 8_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertUploaded(1, 8_000);
        assertThat(localFiles(0), equalTo(0L));

        // and a restart after the last flush keeps the high-water mark, with no local file to find it in
        internalCluster().fullRestart();
        ensureGreen(INDEX);
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(8_000L));
        assertFalse(bulk(8_000, 8_500).hasFailures());
        assertThat(shard(0).seqNoStats().getMaxSeqNo(), equalTo(8_499L));
    }

    public void testBulksAreRejectedWhenTheLocalFilesReachTheLimitUntilAFlushFreesSpace() throws Exception {
        registerRepository();
        // compressed local files of 2,000 documents are a few kilobytes, so the limit is far below that
        createIndex(1, Settings.builder().put("index.parquet.max_local_size", "200b"));

        assertFalse(bulk(0, 2_000).hasFailures());
        final BulkResponse rejected = bulk(2_000, 2_100);
        assertTrue(rejected.hasFailures());
        // the client is told to retry later, with the reason in the cause
        assertThat(rejected.getItems()[0].getFailure().getStatus(), equalTo(org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS));
        assertThat(
            org.elasticsearch.ExceptionsHelper.stackTrace(rejected.getItems()[0].getFailure().getCause()),
            containsString("index.parquet.max_local_size")
        );

        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulk(2_000, 2_100).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertUploaded(1, 2_100);
    }

    public void testAFlushThatFailsKeepsTheRowsLocallyAndAFlushLaterSucceeds() throws Exception {
        // no repository yet: the flush cannot open the table (with staged durability the bulk would already fail)
        createIndex(1, Settings.builder().put("index.parquet.durability", "local"));
        assertFalse(bulk(0, 1_000).hasFailures());

        final BroadcastResponse failed = indicesAdmin().prepareFlush(INDEX).get();
        assertThat(failed.getFailedShards(), equalTo(1));
        assertThat(localFiles(0), greaterThan(0L));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(1_000L));

        registerRepository();
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertUploaded(1, 1_000);
        assertThat(localFiles(0), equalTo(0L));
        assertThat(String.valueOf(RepositoryFileIO.PURPOSE), containsString("INDICES"));
        assertThat(LocalParquetFiles.TEMP_PREFIX, containsString("pqtmp"));
    }

    // ---- the catalog ----

    /** The catalog of the repository, as a reader outside the engine opens it: from the repository directory. */
    private RepositoryCatalog catalog() throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        return new RepositoryCatalog(new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true), BlobPath.EMPTY, root);
    }

    private int newestMetadataVersion() throws IOException {
        final Path metadata = repoRoot.resolve(REPOSITORY).resolve(INDEX).resolve(resolveIndex(INDEX).getUUID()).resolve("metadata");
        int newest = 0;
        try (var files = java.nio.file.Files.list(metadata)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                final var matcher = java.util.regex.Pattern.compile("v(\\d+)\\.metadata\\.json").matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    newest = Math.max(newest, Integer.parseInt(matcher.group(1)));
                }
            }
        }
        return newest;
    }

    public void testEveryFlushCreatesOrUpdatesTheEntryOfTheTableInTheCatalog() throws Exception {
        registerRepository();
        createIndex(2, Settings.builder().put("index.parquet.compaction.enabled", false));
        assertFalse(bulk(0, 6_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        // created by the first flush: it names the index, the table, and a metadata file that exists
        final CatalogEntry first = catalog().read(List.of("default"), INDEX).orElseThrow();
        assertThat(first.index(), equalTo(INDEX));
        assertThat(first.indexUuid(), equalTo(resolveIndex(INDEX).getUUID()));
        assertThat(first.clusterUuid(), equalTo(clusterService().state().metadata().clusterUUID()));
        assertTrue(first.location().endsWith("/" + INDEX + "/" + resolveIndex(INDEX).getUUID()));
        final Path metadataFile = org.elasticsearch.core.PathUtils.get(java.net.URI.create(first.metadataLocation()));
        assertTrue("the metadata file of the entry exists", java.nio.file.Files.exists(metadataFile));
        // two shards committed at nearly the same moment: the entry may lag by a version, and a reader who loads it gets the newest table
        assertThat(catalog().load(List.of("default"), INDEX).orElseThrow().metadataVersion(), equalTo(newestMetadataVersion()));

        // updated by the next flush: the same table, the same creation time, a newer metadata file
        assertFalse(bulk(6_000, 8_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        final CatalogEntry second = catalog().read(List.of("default"), INDEX).orElseThrow();
        assertThat(second.tableUuid(), equalTo(first.tableUuid()));
        assertThat(second.createdAtMillis(), equalTo(first.createdAtMillis()));
        assertThat(second.metadataVersion(), greaterThan(first.metadataVersion()));
        assertThat(catalog().load(List.of("default"), INDEX).orElseThrow().metadataVersion(), equalTo(newestMetadataVersion()));
        assertThat(catalog().list(List.of("default")), org.hamcrest.Matchers.hasItem(INDEX));
    }

    public void testTheNamespaceSettingMovesTheEntryAndTheCatalogCanBeSwitchedOff() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder().put("index.parquet.catalog.namespace", "prod.eu").put("index.parquet.compaction.enabled", false));
        assertFalse(bulk(0, 1_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertTrue(catalog().read(List.of("prod", "eu"), INDEX).isPresent());
        assertTrue(
            java.nio.file.Files.exists(
                repoRoot.resolve(REPOSITORY).resolve("_catalog").resolve("prod").resolve("eu").resolve(INDEX + ".json")
            )
        );
        // the repository directory is shared by the tests of this class, so an entry of the same name in the default namespace can be there
        // from an earlier test: it must not be the entry of this index
        assertThat(
            catalog().read(List.of("default"), INDEX).map(CatalogEntry::indexUuid).orElse("none"),
            not(equalTo(resolveIndex(INDEX).getUUID()))
        );
    }

    public void testAnIndexWithoutACatalogWritesNoEntry() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder().put("index.parquet.catalog.enabled", false).put("index.parquet.catalog.namespace", "off"));
        assertFalse(bulk(0, 1_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        // the table is committed and there is no entry of this index in any namespace
        assertThat(newestMetadataVersion(), greaterThan(1));
        assertFalse(java.nio.file.Files.exists(repoRoot.resolve(REPOSITORY).resolve("_catalog").resolve("off")));
    }

    // ---- one table, one index ----

    public void testAnIndexThatUsesTheBasePathOfAnotherIndexIsRefusedAndKeepsItsRowsLocally() throws Exception {
        registerRepository();
        // the repository directory is shared by the tests of this class, and a table belongs to one index: the path is new for every run
        final String sharedPath = "shared/table-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        createIndex(1, Settings.builder().put("index.parquet.base_path", sharedPath).put("index.parquet.compaction.enabled", false));
        assertFalse(bulk(0, 500).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        // a second index with the same base path would write into the table of the first, with its own sequence numbers
        final String other = "parquet-flush-other";
        assertAcked(
            indicesAdmin().prepareCreate(other)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", "local")
                        .put("index.parquet.base_path", sharedPath)
                )
                .setMapping("count", "type=long")
        );
        ensureGreen(other);
        assertFalse(client().prepareBulk().add(new IndexRequest(other).source("count", 1)).get().hasFailures());

        final BroadcastResponse failed = indicesAdmin().prepareFlush(other).get();
        assertThat(failed.getFailedShards(), equalTo(1));
        assertThat(
            org.elasticsearch.ExceptionsHelper.stackTrace(failed.getShardFailures()[0].getCause()),
            containsString("belongs to the index [" + INDEX + "/" + resolveIndex(INDEX).getUUID() + "]")
        );
        // the rows of the other index are safe in its local files, and the table of the first index is untouched
        assertThat(indicesAdmin().prepareStats(other).get().getTotal().getDocs().getCount(), equalTo(1L));
        assertThat(localFilesOf(other), greaterThan(0L));
        // the table at the shared path holds the one data file of the first index, below the history of its shard
        final Path data = repoRoot.resolve(REPOSITORY).resolve(sharedPath).resolve("data").resolve("shard-0");
        try (var files = java.nio.file.Files.walk(data)) {
            assertThat(files.filter(f -> f.toString().endsWith(".parquet")).count(), equalTo(1L));
        }
    }
}
