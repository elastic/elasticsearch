/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Types;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ShardTableTests.IcebergWorkerPoolFilter.class }
)
public class ShardTableTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String ROOT = "file:///warehouse/logs";
    private static final TableLocation LOCATION = new TableLocation(ROOT, BlobPath.EMPTY.add("logs"));

    private FsBlobStore store;
    private Directory directory;

    @Before
    public void createStores() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
        directory = new ByteBuffersDirectory();
    }

    @After
    public void closeStores() throws Exception {
        store.close();
        directory.close();
    }

    private ShardTable open(int shard) {
        return TestTables.open(store, LOCATION, shard);
    }

    /** A merged file for the range, in the local directory, as the flusher produces it. */
    private LocalParquetFiles.LocalFile mergedFile(ShardTable table, long from, long to, boolean extraColumn) throws IOException {
        final LocalParquetFiles.LocalFile local = TestLocalFiles.write(directory, from, to, extraColumn);
        final MessageType union = ParquetMerger.unionSchema(directory, List.of(local));
        final MessageType withIds = ShardTable.withFieldIds(union, table.ensureColumns(union));
        final String name = LocalParquetFiles.mergedName(from, to);
        final ParquetMerger.Merged merged = ParquetMerger.merge(
            directory,
            List.of(local),
            withIds,
            name,
            CompressionCodecName.SNAPPY,
            1 << 20
        );
        return new LocalParquetFiles.LocalFile(merged.name(), from, to, merged.length());
    }

    private void uploadAndAppend(ShardTable table, LocalParquetFiles.LocalFile merged) throws IOException {
        final String attempt = ShardTable.newAttempt();
        try (IndexInput in = directory.openInput(merged.name(), IOContext.DEFAULT)) {
            table.upload(
                merged.fromSeqNo(),
                merged.toSeqNo(),
                attempt,
                new java.io.BufferedInputStream(new InputStreamIndexInput(in, merged.length())),
                merged.length()
            );
        }
        final DataFile file = table.dataFile(
            merged.fromSeqNo(),
            merged.toSeqNo(),
            attempt,
            merged.length(),
            new IndexInputFile(directory, merged.name())
        );
        table.append(file, merged.toSeqNo());
    }

    public void testColumnsGetStableIdsInTheOrderTheyAppear() throws Exception {
        final ShardTable table = open(0);
        final MessageType schema = TestLocalFiles.schema(false);

        final Map<String, Integer> ids = table.ensureColumns(schema);

        assertThat(ids, equalTo(Map.of("count", 1, "@timestamp", 2, "host.name", 3)));
        assertThat(table.ensureColumns(schema), equalTo(ids));
        // a later mapping update adds a column: it gets the next id, and the others keep theirs
        final Map<String, Integer> later = table.ensureColumns(TestLocalFiles.schema(true));
        assertThat(later, equalTo(Map.of("count", 1, "@timestamp", 2, "host.name", 3, "extra", 4)));
        // the column with a dot in its name is a top-level column with that literal name, and not a path
        assertThat(
            table.schema().columns().stream().map(Types.NestedField::name).toList(),
            equalTo(List.of("count", "@timestamp", "host.name", "extra"))
        );
        assertThat(table.schema().findField("count").type(), equalTo(Types.LongType.get()));
        assertThat(table.schema().columns().get(1).type(), equalTo(Types.TimestampType.withZone()));
        assertThat(table.schema().columns().get(2).type(), equalTo(Types.StringType.get()));
    }

    public void testAListColumnGetsTheIdOfTheListAndTheIdOfItsElementAndKeepsThemInTheFiles() throws Exception {
        final ShardTable table = open(0);
        final MessageType schema = org.apache.parquet.schema.Types.buildMessage()
            .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("n"))
            .addField(
                org.elasticsearch.xpack.parquet.write.file.ParquetLists.listOf(
                    "tags",
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY,
                    org.apache.parquet.schema.LogicalTypeAnnotation.stringType()
                )
            )
            .named("doc");

        final Map<String, Integer> ids = table.ensureColumns(schema);

        final Types.NestedField tags = table.schema().findField("tags");
        assertTrue(tags.type().isListType());
        assertThat(tags.type().asListType().elementType(), equalTo(Types.StringType.get()));
        assertThat(tags.type().asListType().isElementOptional(), equalTo(true));
        assertThat(ids.get("tags"), equalTo(tags.fieldId()));
        assertThat(ids.get(ShardTable.elementKey("tags")), equalTo(tags.type().asListType().elementId()));
        assertThat(
            "the element has an id of its own",
            ids.get(ShardTable.elementKey("tags")),
            org.hamcrest.Matchers.not(equalTo(tags.fieldId()))
        );
        assertThat(table.ensureColumns(schema), equalTo(ids));

        // the Parquet schema of the file carries both ids, and the repeated group in between has none
        final MessageType withIds = ShardTable.withFieldIds(schema, ids);
        final org.apache.parquet.schema.GroupType list = withIds.getType("tags").asGroupType();
        assertThat(list.getId().intValue(), equalTo(tags.fieldId()));
        assertNull(list.getType(org.elasticsearch.xpack.parquet.write.file.ParquetLists.LIST).getId());
        assertThat(
            org.elasticsearch.xpack.parquet.write.file.ParquetLists.elementOf(withIds.getType("tags")).getId().intValue(),
            equalTo(tags.type().asListType().elementId())
        );
        assertThat(withIds.getType("n").getId().intValue(), equalTo(ids.get("n")));
    }

    public void testAColumnCannotChangeBetweenFlatAndAList() throws Exception {
        final ShardTable table = open(0);
        final MessageType flat = org.apache.parquet.schema.Types.buildMessage()
            .addField(
                org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("tags")
            )
            .named("doc");
        table.ensureColumns(flat);
        final MessageType list = org.apache.parquet.schema.Types.buildMessage()
            .addField(
                org.elasticsearch.xpack.parquet.write.file.ParquetLists.listOf(
                    "tags",
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY,
                    org.apache.parquet.schema.LogicalTypeAnnotation.stringType()
                )
            )
            .named("doc");

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> table.ensureColumns(list));

        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("column [tags] is a list in the file and not in the table"));
    }

    public void testShardsThatOpenAndAddColumnsAtTheSameTimeAgreeOnTheIds() throws Exception {
        final int shards = 6;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Map<String, Integer>> results = java.util.Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger failures = new AtomicInteger();
        final List<Thread> threads = new ArrayList<>();
        for (int shard = 0; shard < shards; shard++) {
            final int shardId = shard;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    final ShardTable table = open(shardId);
                    results.add(table.ensureColumns(TestLocalFiles.schema(shardId % 2 == 0)));
                } catch (Throwable t) {
                    failures.incrementAndGet();
                    logger.error("shard [" + shardId + "] failed", t);
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
        assertThat(results, hasSize(shards));
        // whatever the order of the shards, every shard sees the same id for the same column
        for (Map<String, Integer> result : results) {
            for (Map.Entry<String, Integer> column : result.entrySet()) {
                assertThat(column.getKey(), column.getValue(), equalTo(results.get(0).getOrDefault(column.getKey(), column.getValue())));
            }
        }
        final ShardTable reader = open(0);
        assertThat(reader.schema().columns(), hasSize(4));
        assertThat(reader.schema().columns().stream().map(Types.NestedField::fieldId).toList(), containsInAnyOrder(1, 2, 3, 4));
    }

    public void testUploadAndAppendMakeTheRowsVisibleAndSetTheHighWaterMark() throws Exception {
        final ShardTable table = open(0);
        assertThat(table.uploadedThroughSeqNo(), equalTo(-1L));
        assertThat(table.uploadedFiles(), hasSize(0));

        uploadAndAppend(table, mergedFile(table, 0, 99, false));
        uploadAndAppend(table, mergedFile(table, 100, 249, false));

        assertThat(table.uploadedThroughSeqNo(), equalTo(249L));
        final List<ShardTable.UploadedFile> files = table.uploadedFiles();
        assertThat(files, hasSize(2));
        assertThat(files.get(0).fromSeqNo(), equalTo(0L));
        assertThat(files.get(0).rows(), equalTo(100L));
        assertThat(files.get(1).toSeqNo(), equalTo(249L));
        assertThat(files.get(1).rows(), equalTo(150L));
        assertThat(
            files.get(1).location(),
            startsWith(table.historyLocation() + String.format(java.util.Locale.ROOT, "%019d-%019d-", 100, 249))
        );
    }

    public void testAReaderSeesTheFilesAndTheVersionThatTheWritersCommitted() throws Exception {
        final TableReader reader = TableReader.open(store, LOCATION);
        // the table does not exist yet, and the reader does not create it (the test file system may add extra files, which are no
        // metadata files)
        assertThat(reader.read(), equalTo(TableReader.Contents.NONE));
        assertThat(
            store.blobContainer(LOCATION.blobPath().add("metadata"))
                .listBlobs(RepositoryFileIO.PURPOSE)
                .keySet()
                .stream()
                .filter(name -> RepositoryTableOperations.versionOfMetadataFile(name) >= 0)
                .toList(),
            hasSize(0)
        );

        final ShardTable shard0 = open(0);
        final ShardTable shard1 = open(1);
        uploadAndAppend(shard0, mergedFile(shard0, 0, 49, false));
        final long afterShard0 = shard0.metadataVersion();
        uploadAndAppend(shard1, mergedFile(shard1, 0, 199, false));
        final long afterShard1 = shard1.metadataVersion();
        assertThat(afterShard1, greaterThan(afterShard0));

        final TableReader.Contents contents = reader.read();
        assertThat(contents.version(), equalTo(afterShard1));
        assertThat(TableReader.ownerIndexUuid(contents.properties()), equalTo(TestTables.INDEX_UUID));
        assertThat(contents.files(), hasSize(2));
        long rows = 0;
        for (TableReader.DataFileEntry file : contents.files()) {
            final TableReader.ShardFileName name = TableReader.parseShardFileName(file.location());
            assertThat(name.historyUuid(), equalTo(TestTables.HISTORY));
            assertThat(name.fromSeqNo(), equalTo(0L));
            assertThat(file.rows(), equalTo(name.shard() == 0 ? 50L : 200L));
            assertThat(file.pathInTable(LOCATION), startsWith("data/shard-" + name.shard() + "/" + TestTables.HISTORY + "/"));
            rows += file.rows();
        }
        assertThat(rows, equalTo(250L));
        // a writer that has committed version N knows that a reader of version N or later sees its files
        assertThat(contents.version(), greaterThanOrEqualTo(shard0.metadataVersion()));
    }

    /**
     * A table that another tool wrote through a catalog names its metadata files {@code <NNNNN>-<uuid>.metadata.json}, starting at 0. The
     * test gives the files of a table of ours those names, as pyiceberg would have written them, and the reader must see the same files,
     * with the version of the last file plus one. A writer refuses to commit to such a table.
     */
    public void testAReaderReadsATableWhoseMetadataFilesACatalogNamed() throws Exception {
        final ShardTable shard0 = open(0);
        uploadAndAppend(shard0, mergedFile(shard0, 0, 49, false));
        uploadAndAppend(shard0, mergedFile(shard0, 50, 149, false));
        final TableReader reader = TableReader.open(store, LOCATION);
        final TableReader.Contents expected = reader.read();
        assertThat(expected.files(), hasSize(2));

        final BlobContainer metadata = store.blobContainer(LOCATION.blobPath().add("metadata"));
        final List<String> renamed = new ArrayList<>();
        for (String name : metadata.listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            final int version = RepositoryTableOperations.versionOfMetadataFile(name);
            if (name.startsWith("v") && version > 0) {
                final String catalogName = Strings.format("%05d-%s.metadata.json", version - 1, UUID.randomUUID());
                final byte[] bytes;
                try (InputStream in = metadata.readBlob(RepositoryFileIO.PURPOSE, name)) {
                    bytes = in.readAllBytes();
                }
                metadata.writeBlob(RepositoryFileIO.PURPOSE, catalogName, new BytesArray(bytes), true);
                metadata.deleteBlobsIgnoringIfNotExists(RepositoryFileIO.PURPOSE, List.of(name).iterator());
                renamed.add(catalogName);
            }
        }
        assertThat(renamed, hasSize((int) expected.version()));

        final TableReader.Contents contents = TableReader.open(store, LOCATION).read();
        assertThat(contents.version(), equalTo(expected.version()));
        // the order of the manifests is not fixed from one read to the next
        assertThat(contents.files(), containsInAnyOrder(expected.files().toArray()));
        assertThat(contents.properties(), equalTo(expected.properties()));

        // the catalog of the other tool decides which file is current, so we never write to its table
        final Exception e = expectThrows(Exception.class, () -> uploadAndAppend(open(0), mergedFile(shard0, 150, 199, false)));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("was written through a catalog"));
    }

    public void testTheVersionOfAMetadataFile() {
        assertThat(RepositoryTableOperations.versionOfMetadataFile("v1.metadata.json"), equalTo(1));
        assertThat(RepositoryTableOperations.versionOfMetadataFile("v12.metadata.json"), equalTo(12));
        assertThat(RepositoryTableOperations.versionOfMetadataFile("00000-6f5c8a3e-2b1d-4c7e-9a0f-1d2e3f4a5b6c.metadata.json"), equalTo(1));
        assertThat(
            RepositoryTableOperations.versionOfMetadataFile("00041-6f5c8a3e-2b1d-4c7e-9a0f-1d2e3f4a5b6c.gz.metadata.json"),
            equalTo(42)
        );
        assertThat(RepositoryTableOperations.versionOfMetadataFile("version-hint.text"), equalTo(-1));
        assertThat(RepositoryTableOperations.versionOfMetadataFile("snap-1-1-6f5c8a3e.avro"), equalTo(-1));
        assertThat(TableReader.versionOf("file:///t/metadata/00002-6f5c8a3e-2b1d-4c7e-9a0f-1d2e3f4a5b6c.metadata.json"), equalTo(3L));
        assertThat(TableReader.versionOf("s3://b/t/metadata/v7.metadata.json"), equalTo(7L));
    }

    public void testShardsSeeOnlyTheirOwnFiles() throws Exception {
        final ShardTable shard0 = open(0);
        final ShardTable shard1 = open(1);

        uploadAndAppend(shard0, mergedFile(shard0, 0, 49, false));
        uploadAndAppend(shard1, mergedFile(shard1, 0, 199, false));
        uploadAndAppend(shard0, mergedFile(shard0, 50, 99, false));

        assertThat(shard0.uploadedThroughSeqNo(), equalTo(99L));
        assertThat(shard1.uploadedThroughSeqNo(), equalTo(199L));
        assertThat(shard0.uploadedFiles(), hasSize(2));
        assertThat(shard1.uploadedFiles(), hasSize(1));
        // a shard that opens the table later sees the same
        assertThat(open(1).uploadedThroughSeqNo(), equalTo(199L));
    }

    public void testAKeyIsWrittenOnce() throws Exception {
        final ShardTable table = open(0);
        final LocalParquetFiles.LocalFile merged = mergedFile(table, 0, 9, false);
        final byte[] bytes = new byte[] { 1, 2, 3 };
        table.upload(0, 9, "same", new java.io.ByteArrayInputStream(bytes), bytes.length);

        // a second write to the key fails and leaves the first bytes
        expectThrows(java.io.IOException.class, () -> table.upload(0, 9, "same", new java.io.ByteArrayInputStream(bytes), bytes.length));
        try (IndexInput in = directory.openInput(merged.name(), IOContext.DEFAULT)) {
            expectThrows(
                java.io.IOException.class,
                () -> table.upload(
                    0,
                    9,
                    "same",
                    new java.io.BufferedInputStream(new InputStreamIndexInput(in, merged.length())),
                    merged.length()
                )
            );
        }
        try (var in = TestTables.read(store, LOCATION, table.dataLocation(0, 9, "same"))) {
            assertThat(in.readAllBytes(), equalTo(bytes));
        }
    }

    public void testAShardWithANewHistoryDoesNotReplaceOrSeeTheFilesOfTheOldOne() throws Exception {
        final ShardTable old = TestTables.open(store, LOCATION, 0, "history-old");
        uploadAndAppend(old, mergedFile(old, 0, 99, false));
        final ShardTable.UploadedFile oldFile = old.uploadedFiles().get(0);
        final byte[] oldBytes;
        try (var in = TestTables.read(store, LOCATION, oldFile.location())) {
            oldBytes = in.readAllBytes();
        }

        // the store of the shard is new, so its sequence numbers start again at 0 and it writes the same range
        for (String name : directory.listAll()) {
            directory.deleteFile(name); // the new store has none of the local files of the old one
        }
        final ShardTable fresh = TestTables.open(store, LOCATION, 0, "history-new");
        assertThat("the new history has no file and no high-water mark", fresh.uploadedFiles(), hasSize(0));
        assertThat(fresh.uploadedThroughSeqNo(), equalTo(-1L));
        uploadAndAppend(fresh, mergedFile(fresh, 0, 99, false));

        final ShardTable.UploadedFile freshFile = fresh.uploadedFiles().get(0);
        assertThat(fresh.uploadedFiles(), hasSize(1));
        assertThat(old.uploadedFiles(), hasSize(1));
        assertThat(freshFile.location(), not(equalTo(oldFile.location())));
        assertThat(freshFile.location(), containsString("/history-new/"));
        try (var in = TestTables.read(store, LOCATION, oldFile.location())) {
            assertThat("the file of the old history is untouched", in.readAllBytes(), equalTo(oldBytes));
        }
        // the table lists both, and a listing of the exact key tells them apart
        assertTrue(fresh.isListed(oldFile.location()));
        assertTrue(fresh.isListed(freshFile.location()));
        assertFalse(fresh.isListed(fresh.dataLocation(0, 99, "no-such-attempt")));
    }

    public void testATableOfAnotherIndexIsRefused() throws Exception {
        open(0); // creates the table for the index of the tests

        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ShardTable.open(store, LOCATION, 0, "other", "other-uuid", TestTables.HISTORY, null)
        );

        assertThat(e.getMessage(), containsString("belongs to the index [" + TestTables.INDEX + "/" + TestTables.INDEX_UUID + "]"));
        assertThat(e.getMessage(), containsString("[other/other-uuid]"));
    }

    public void testATableThatNamesNoOwnerIsRefused() throws Exception {
        final RepositoryFileIO io = new RepositoryFileIO(store, LOCATION.blobPath(), LOCATION.rootUri());
        new RepositoryTableOperations(io).commit(
            null,
            org.apache.iceberg.TableMetadata.newTableMetadata(
                new org.apache.iceberg.Schema(),
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                org.apache.iceberg.SortOrder.unsorted(),
                LOCATION.rootUri(),
                Map.of()
            )
        );

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> open(0));

        assertThat(e.getMessage(), containsString("does not name the index that owns it"));
    }

    // ---- staged files ----

    public void testStagedFilesAreListedByRangeAndDeleted() throws Exception {
        final ShardTable table = open(0);
        final byte[] bytes = new byte[] { 1, 2, 3 };

        final String second = table.stageFile(100, 149, "b", new java.io.ByteArrayInputStream(bytes), bytes.length);
        final String first = table.stageFile(0, 99, "a", new java.io.ByteArrayInputStream(bytes), bytes.length);

        final List<ShardTable.StagedFile> staged = table.listStaged();
        assertThat(staged, hasSize(2));
        assertThat(staged.get(0).name(), equalTo(first));
        assertThat(staged.get(0).fromSeqNo(), equalTo(0L));
        assertThat(staged.get(0).rows(), equalTo(100L));
        assertThat(staged.get(0).bytes(), equalTo(3L));
        assertThat(staged.get(1).name(), equalTo(second));
        assertThat(staged.get(1).toSeqNo(), equalTo(149L));
        assertThat("staged files are not data files", table.uploadedFiles(), hasSize(0));

        table.deleteStaged(List.of(first, "no-such-blob.parquet"));

        assertThat(table.listStaged().stream().map(ShardTable.StagedFile::name).toList(), equalTo(List.of(second)));
    }

    public void testAStagedKeyIsWrittenOnce() throws Exception {
        final ShardTable table = open(0);
        final byte[] bytes = new byte[] { 1 };
        table.stageFile(0, 9, "same", new java.io.ByteArrayInputStream(bytes), bytes.length);

        expectThrows(java.io.IOException.class, () -> table.stageFile(0, 9, "same", new java.io.ByteArrayInputStream(bytes), bytes.length));
    }

    public void testStagedFilesOfAnotherHistoryAreNotListed() throws Exception {
        final ShardTable old = TestTables.open(store, LOCATION, 0, "history-old");
        final ShardTable fresh = TestTables.open(store, LOCATION, 0, "history-new");
        final byte[] bytes = new byte[] { 1 };

        old.stageFile(0, 9, "a", new java.io.ByteArrayInputStream(bytes), bytes.length);

        assertThat(old.listStaged(), hasSize(1));
        assertThat(fresh.listStaged(), hasSize(0));
        assertThat(old.stagingLocation(), containsString("/staging/shard-0/history-old/"));
    }
}
