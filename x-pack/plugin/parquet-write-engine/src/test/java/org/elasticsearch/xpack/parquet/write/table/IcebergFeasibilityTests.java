/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Spike S0: can an Iceberg table live in the blob store of a snapshot repository, with our own {@link RepositoryFileIO} and
 * {@link RepositoryTableOperations}, and can it carry the appends of a flush and the rewrite of a compaction? The blob store here is the
 * {@code fs} repository, whose atomic write that fails if the blob exists is a hard link.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        IcebergFeasibilityTests.IcebergWorkerPoolFilter.class }
)
public class IcebergFeasibilityTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String ROOT = "file:///warehouse/logs";
    private static final Schema SCHEMA = new Schema(
        NestedField.optional(1, "count", LongType.get()),
        NestedField.optional(2, "@timestamp", TimestampType.withZone()),
        NestedField.optional(3, "host.name", StringType.get())
    );

    private FsBlobStore store;
    private BlobPath base;

    @Before
    public void createStore() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
        base = BlobPath.EMPTY.add("logs");
    }

    @After
    public void closeStore() throws Exception {
        store.close();
    }

    private RepositoryFileIO newIo() {
        return new RepositoryFileIO(store, base, ROOT);
    }

    private Table createTable() {
        final RepositoryTableOperations ops = new RepositoryTableOperations(newIo());
        // Iceberg retries a commit that lost the race (default: 4 retries). Writers that flush at the same moment need more.
        final Map<String, String> properties = Map.of(
            "commit.retry.num-retries",
            "100",
            "commit.retry.min-wait-ms",
            "5",
            "commit.retry.max-wait-ms",
            "100"
        );
        final TableMetadata metadata = TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            ROOT,
            properties
        );
        ops.commit(null, metadata);
        return new BaseTable(ops, "logs");
    }

    /** Another writer: its own file IO and operations over the same blob store, like another shard. */
    private Table openTable() {
        return new BaseTable(new RepositoryTableOperations(newIo()), "logs");
    }

    // ---- a Parquet file with Iceberg field ids, uploaded below data/shard-<n>/ ----

    private DataFile uploadParquetFile(int shard, long from, long to) throws IOException {
        final byte[] bytes = parquetBytes(from, to);
        final String name = String.format(java.util.Locale.ROOT, "%019d-%019d.parquet", from, to);
        final BlobContainer container = store.blobContainer(base.add("data").add("shard-" + shard));
        container.writeBlob(RepositoryFileIO.PURPOSE, name, new BytesArray(bytes), true);

        final String location = ROOT + "/data/shard-" + shard + "/" + name;
        final Metrics metrics = ParquetFileMetrics.read(new MemoryInputFile(bytes));
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(location)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(bytes.length)
            .withMetrics(metrics)
            .build();
    }

    /** Rows {@code from} to {@code to} inclusive: {@code count} is the row number, the other fields derive from it. */
    private static byte[] parquetBytes(long from, long to) throws IOException {
        final MessageType schema = Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).id(1).named("count"))
            .addField(
                Types.optional(PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .id(2)
                    .named("@timestamp")
            )
            .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(3).named("host.name"))
            .named("doc");
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(new MemoryOutputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (long row = from; row <= to; row++) {
                final Group group = factory.newGroup();
                group.add("count", row);
                group.add("@timestamp", 1_700_000_000_000L + row * 1000);
                group.add("host.name", Binary.fromString("host-" + (row % 7)));
                writer.write(group);
            }
        }
        return out.toByteArray();
    }

    private List<Long> readCounts(DataFile file) throws IOException {
        final String name = file.location().substring(file.location().lastIndexOf('/') + 1);
        final String shard = file.location().substring(file.location().lastIndexOf("data/shard-") + 5, file.location().lastIndexOf('/'));
        final BlobContainer container = store.blobContainer(base.add("data").add(shard));
        final byte[] bytes;
        try (var in = container.readBlob(RepositoryFileIO.PURPOSE, name)) {
            bytes = in.readAllBytes();
        }
        final List<Long> counts = new ArrayList<>();
        final ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader reader = ParquetFileReader.open(new MemoryInputFile(bytes), options)) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    counts.add(recordReader.read().getLong("count", 0));
                }
            }
        }
        return counts;
    }

    private static List<String> activeFiles(Table table) throws IOException {
        table.refresh();
        final List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(task.file().location());
            }
        }
        return files;
    }

    /** The data files in a container. The test file system can add files of its own (such as {@code extra0}) to any directory. */
    private static Set<String> dataFiles(BlobContainer container) throws IOException {
        final Set<String> names = new TreeSet<>();
        for (String name : container.listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            if (name.endsWith(".parquet")) {
                names.add(name);
            }
        }
        return names;
    }

    private Set<Integer> metadataVersions() throws IOException {
        final Set<Integer> versions = new TreeSet<>();
        for (String name : store.blobContainer(base.add("metadata")).listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            if (name.matches("v\\d+\\.metadata\\.json")) {
                versions.add(Integer.parseInt(name.substring(1, name.indexOf('.'))));
            }
        }
        return versions;
    }

    // ---- tests ----

    public void testCreateAppendAndRewrite() throws Exception {
        final Table table = createTable();
        assertThat(metadataVersions(), equalTo(Set.of(1)));
        assertThat(activeFiles(table), hasSize(0));

        // two flushes
        final DataFile first = uploadParquetFile(0, 0, 99);
        final DataFile second = uploadParquetFile(0, 100, 249);
        table.newAppend().appendFile(first).commit();
        table.newAppend().appendFile(second).commit();
        assertThat(activeFiles(table), containsInAnyOrder(first.location(), second.location()));
        assertThat(metadataVersions(), equalTo(Set.of(1, 2, 3)));

        // the footer metrics that Iceberg took from our file: rows and the bounds of the field ids
        assertThat(first.recordCount(), equalTo(100L));
        assertThat(second.recordCount(), equalTo(150L));
        assertThat(first.lowerBounds().keySet(), hasItem(1));
        assertThat(first.upperBounds().keySet(), hasItem(1));

        // a compaction: one file with the rows of both replaces them in one commit
        final DataFile compacted = uploadParquetFile(0, 0, 249);
        table.newRewrite().rewriteFiles(Set.of(first, second), Set.of(compacted)).commit();
        assertThat(activeFiles(table), equalTo(List.of(compacted.location())));
        assertThat(table.currentSnapshot().summary().get("total-records"), equalTo("250"));
        assertThat(readCounts(compacted).size(), equalTo(250));

        // a second reader sees the same table
        assertThat(activeFiles(openTable()), equalTo(List.of(compacted.location())));

        // the pointer that readers without a listing use names the newest version
        try (var in = store.blobContainer(base.add("metadata")).readBlob(RepositoryFileIO.PURPOSE, "version-hint.text")) {
            assertThat(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8), equalTo("4"));
        }
        assertThat(metadataVersions(), equalTo(Set.of(1, 2, 3, 4)));
    }

    public void testWritersThatReadTheSameVersionCannotBothCommit() throws Exception {
        final Table table = createTable();
        final RepositoryTableOperations opsA = new RepositoryTableOperations(newIo());
        final RepositoryTableOperations opsB = new RepositoryTableOperations(newIo());
        final TableMetadata baseA = opsA.current();
        final TableMetadata baseB = opsB.current();
        assertThat(baseA.metadataFileLocation(), equalTo(baseB.metadataFileLocation()));

        opsA.commit(baseA, baseA.replaceProperties(Map.of("writer", "a")));
        final CommitFailedException e = expectThrows(
            CommitFailedException.class,
            () -> opsB.commit(baseB, baseB.replaceProperties(Map.of("writer", "b")))
        );
        assertThat(e.getMessage(), not(equalTo(null)));
        assertThat(opsA.refresh().properties().get("writer"), equalTo("a"));
        assertThat(metadataVersions(), equalTo(Set.of(1, 2)));
        assertNotNull(table);
    }

    public void testShardsCommitToOneTableConcurrently() throws Exception {
        createTable();
        final int shards = 6;
        final int flushes = 5;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger failures = new AtomicInteger();
        final List<Thread> threads = new ArrayList<>();
        final List<String> expected = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int shard = 0; shard < shards; shard++) {
            final int shardId = shard;
            final Thread thread = new Thread(() -> {
                try {
                    final Table table = openTable();
                    start.await();
                    for (int flush = 0; flush < flushes; flush++) {
                        final long from = flush * 10L;
                        final DataFile file = uploadParquetFile(shardId, from, from + 9);
                        table.newAppend().appendFile(file).commit();
                        expected.add(file.location());
                    }
                } catch (Throwable t) {
                    failures.incrementAndGet();
                    logger.error("writer of shard [" + shardId + "] failed", t);
                }
            });
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(failures.get(), equalTo(0));

        final Table reader = openTable();
        assertThat(activeFiles(reader), containsInAnyOrder(expected.toArray()));
        assertThat(expected, hasSize(shards * flushes));
        // one commit for the creation and one for each flush, so the versions are 1 to 31 with no gap and no duplicate
        final Set<Integer> versions = new TreeSet<>();
        for (int v = 1; v <= 1 + shards * flushes; v++) {
            versions.add(v);
        }
        assertThat(metadataVersions(), equalTo(versions));
        assertThat(reader.snapshots().iterator().hasNext(), equalTo(true));
        assertThat(reader.currentSnapshot().summary().get("total-records"), equalTo(Long.toString(shards * flushes * 10L)));
    }

    /**
     * Iceberg deletes a file that a rewrite replaced when the snapshot of the rewrite is expired, and the current snapshot is never
     * expired. So the replaced files stay until the next commit (the next flush) and the retention delay have passed. Until then a reader
     * that holds the snapshot from before the rewrite can finish.
     */
    public void testReplacedFilesAreDeletedWhenTheRewriteSnapshotExpires() throws Exception {
        final Table table = createTable();
        final DataFile first = uploadParquetFile(0, 0, 9);
        final DataFile second = uploadParquetFile(0, 10, 19);
        table.newAppend().appendFile(first).commit();
        table.newAppend().appendFile(second).commit();
        final DataFile compacted = uploadParquetFile(0, 0, 19);
        table.newRewrite().rewriteFiles(Set.of(first, second), Set.of(compacted)).commit();

        final BlobContainer shardData = store.blobContainer(base.add("data").add("shard-0"));
        final Set<String> afterRewrite = dataFiles(shardData);
        assertThat(afterRewrite.toString(), afterRewrite, hasSize(3));

        // the rewrite snapshot is the current one, so expiring everything older than now deletes nothing yet
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000).cleanExpiredFiles(true).commit();
        final Set<String> afterFirstExpiry = dataFiles(shardData);
        assertThat(afterFirstExpiry.toString(), afterFirstExpiry, hasSize(3));
        assertThat(table.snapshots().iterator().hasNext(), equalTo(true));

        // the next flush commits a new snapshot, and then the rewrite snapshot can expire
        final DataFile next = uploadParquetFile(0, 20, 29);
        table.newAppend().appendFile(next).commit();
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000).cleanExpiredFiles(true).commit();

        final Set<String> afterSecondExpiry = dataFiles(shardData);
        assertThat(afterSecondExpiry.toString(), afterSecondExpiry, hasSize(2));
        assertThat(activeFiles(table), containsInAnyOrder(compacted.location(), next.location()));
        assertThat(readCounts(compacted).size(), equalTo(20));
        assertThat(readCounts(next).size(), equalTo(10));
    }

    /**
     * Writes a table that other Iceberg readers can open: the locations in its metadata are the real {@code file://} URIs of a directory.
     * It runs only when {@code tests.iceberg.export.dir} names an empty directory, and it leaves the table there for a script
     * (pyiceberg) that reads it independently of our code. The table has an append of two files, a rewrite of both into one,
     * and one more append, so its history has an add, a replace and a second add.
     */
    public void testExportATableForIndependentReaders() throws Exception {
        final String exportDir = System.getProperty("tests.iceberg.export.dir");
        assumeTrue("set tests.iceberg.export.dir to export a table", exportDir != null);
        final java.nio.file.Path root = org.elasticsearch.core.PathUtils.get(exportDir);
        assertTrue(
            "the directory must be empty",
            java.nio.file.Files.notExists(root) || java.nio.file.Files.list(root).findAny().isEmpty()
        );
        java.nio.file.Files.createDirectories(root);

        final String location = root.toUri().toString().replaceAll("/$", "");
        try (FsBlobStore exportStore = new FsBlobStore(4096, root, false)) {
            final RepositoryFileIO io = new RepositoryFileIO(exportStore, BlobPath.EMPTY, location);
            final RepositoryTableOperations ops = new RepositoryTableOperations(io);
            ops.commit(
                null,
                TableMetadata.newTableMetadata(SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), location, Map.of())
            );
            final Table table = new BaseTable(ops, "exported");

            final DataFile first = exportedFile(exportStore, io, location, 0, 99);
            final DataFile second = exportedFile(exportStore, io, location, 100, 249);
            table.newAppend().appendFile(first).commit();
            table.newAppend().appendFile(second).commit();
            final DataFile compacted = exportedFile(exportStore, io, location, 0, 249);
            table.newRewrite().rewriteFiles(Set.of(first, second), Set.of(compacted)).commit();
            final DataFile next = exportedFile(exportStore, io, location, 250, 299);
            table.newAppend().appendFile(next).commit();
        }
    }

    private static DataFile exportedFile(FsBlobStore exportStore, RepositoryFileIO io, String location, long from, long to)
        throws IOException {
        final byte[] bytes = parquetBytes(from, to);
        final String name = String.format(java.util.Locale.ROOT, "%019d-%019d.parquet", from, to);
        exportStore.blobContainer(BlobPath.EMPTY.add("data").add("shard-0"))
            .writeBlob(RepositoryFileIO.PURPOSE, name, new BytesArray(bytes), true);
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(location + "/data/shard-0/" + name)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(bytes.length)
            .withMetrics(ParquetFileMetrics.read(new MemoryInputFile(bytes)))
            .build();
    }

    /** Every commit writes a metadata file, and the files older than the configured number of previous versions are deleted. */
    public void testOldMetadataVersionsAreDeleted() throws Exception {
        final RepositoryTableOperations ops = new RepositoryTableOperations(newIo());
        ops.commit(
            null,
            TableMetadata.newTableMetadata(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ROOT,
                Map.of("write.metadata.previous-versions-max", "3")
            )
        );
        final Table table = new BaseTable(ops, "logs");
        for (int i = 0; i < 12; i++) {
            table.updateProperties().set("commit", Integer.toString(i)).commit();
        }

        // 13 versions were written: the current one and 3 previous ones stay
        assertThat(metadataVersions(), equalTo(Set.of(10, 11, 12, 13)));
        // a new reader finds the table through the listing and the hint, and sees the last commit
        assertThat(openTable().properties().get("commit"), equalTo("11"));
    }

    /**
     * The S3 repository reports a conditional write that lost the race as a plain IOException (it wraps an SDK exception), and not as a
     * FileAlreadyExistsException. The commit must still see it as a lost race, so that Iceberg retries.
     */
    public void testAConditionalWriteThatFailsWithAnIoExceptionIsALostRace() throws Exception {
        final S3LikeBlobStore s3Like = new S3LikeBlobStore(store);
        createTable();
        final RepositoryTableOperations loser = new RepositoryTableOperations(new RepositoryFileIO(s3Like, base, ROOT));
        final TableMetadata loserBase = loser.current();
        final RepositoryTableOperations winner = new RepositoryTableOperations(newIo());
        final TableMetadata winnerBase = winner.current();

        // the winner commits between the check of the loser and its write
        s3Like.beforeNextWrite = () -> winner.commit(winnerBase, winnerBase.replaceProperties(Map.of("writer", "winner")));
        final CommitFailedException e = expectThrows(
            CommitFailedException.class,
            () -> loser.commit(loserBase, loserBase.replaceProperties(Map.of("writer", "loser")))
        );
        assertThat(e.getCause().getMessage(), containsString("Unable to upload"));
        assertThat(loser.refresh().properties().get("writer"), equalTo("winner"));
        assertThat(metadataVersions(), equalTo(Set.of(1, 2)));
    }

    /** A response that is lost after the server stored the blob must not make the writer retry, which would add the change twice. */
    public void testALostResponseAfterASuccessfulWriteCountsAsCommitted() throws Exception {
        final S3LikeBlobStore s3Like = new S3LikeBlobStore(store);
        createTable();
        final RepositoryTableOperations ops = new RepositoryTableOperations(new RepositoryFileIO(s3Like, base, ROOT));
        final TableMetadata current = ops.current();

        s3Like.loseResponseOfNextWrite = true;
        ops.commit(current, current.replaceProperties(Map.of("writer", "me")));

        assertThat(ops.refresh().properties().get("writer"), equalTo("me"));
        assertThat(metadataVersions(), equalTo(Set.of(1, 2)));
    }

    /** A write that fails and leaves nothing behind is a real failure, and not a lost race. */
    public void testAFailedWriteThatLeavesNothingIsAnError() throws Exception {
        final S3LikeBlobStore s3Like = new S3LikeBlobStore(store);
        createTable();
        final RepositoryTableOperations ops = new RepositoryTableOperations(new RepositoryFileIO(s3Like, base, ROOT));
        final TableMetadata current = ops.current();

        s3Like.failNextWriteWithoutStoring = true;
        expectThrows(RuntimeIOException.class, () -> ops.commit(current, current.replaceProperties(Map.of("writer", "me"))));
        assertThat(metadataVersions(), equalTo(Set.of(1)));
    }

    /** A blob store that behaves like the S3 repository for a conditional write, and can inject the failures of a network. */
    private static final class S3LikeBlobStore implements BlobStore {
        private final BlobStore delegate;
        Runnable beforeNextWrite;
        boolean loseResponseOfNextWrite;
        boolean failNextWriteWithoutStoring;

        S3LikeBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new FilterBlobContainer(delegate.blobContainer(path)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
                    throws IOException {
                    if (blobName.startsWith("v") && blobName.endsWith(".metadata.json")) {
                        final Runnable before = beforeNextWrite;
                        beforeNextWrite = null;
                        if (before != null) {
                            before.run();
                        }
                        if (failNextWriteWithoutStoring) {
                            failNextWriteWithoutStoring = false;
                            throw new IOException("Unable to upload object [" + blobName + "] using a single upload");
                        }
                        if (failIfAlreadyExists && blobExists(purpose, blobName)) {
                            // what the S3 repository does with an HTTP 412
                            throw new IOException(
                                "Unable to upload object [" + blobName + "] using a single upload",
                                new RuntimeException("412")
                            );
                        }
                        super.writeBlobAtomic(purpose, blobName, bytes, failIfAlreadyExists);
                        if (loseResponseOfNextWrite) {
                            loseResponseOfNextWrite = false;
                            throw new IOException("connection reset after the blob was stored");
                        }
                    } else {
                        super.writeBlobAtomic(purpose, blobName, bytes, failIfAlreadyExists);
                    }
                }
            };
        }

        @Override
        public void close() throws IOException {
            // the delegate is closed by the test
        }
    }

    // ---- small Parquet adapters over memory ----

    private record MemoryOutputFile(ByteArrayOutputStream out) implements OutputFile {
        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                @Override
                public long getPos() {
                    return out.size();
                }

                @Override
                public void write(int b) {
                    out.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    out.write(b, off, len);
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private record MemoryInputFile(byte[] bytes) implements InputFile {
        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableInputStream() {
                private final ByteBuffer buffer = ByteBuffer.wrap(bytes);

                @Override
                public long getPos() {
                    return buffer.position();
                }

                @Override
                public void seek(long newPos) {
                    buffer.position((int) newPos);
                }

                @Override
                public void readFully(byte[] destination) throws IOException {
                    readFully(destination, 0, destination.length);
                }

                @Override
                public void readFully(byte[] destination, int start, int len) throws IOException {
                    if (buffer.remaining() < len) {
                        throw new EOFException();
                    }
                    buffer.get(destination, start, len);
                }

                @Override
                public int read(ByteBuffer destination) {
                    if (buffer.hasRemaining() == false) {
                        return -1;
                    }
                    final int length = Math.min(destination.remaining(), buffer.remaining());
                    destination.put(buffer.slice().limit(length));
                    buffer.position(buffer.position() + length);
                    return length;
                }

                @Override
                public void readFully(ByteBuffer destination) throws IOException {
                    if (buffer.remaining() < destination.remaining()) {
                        throw new EOFException();
                    }
                    read(destination);
                }

                @Override
                public int read() {
                    return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
                }
            };
        }
    }
}
