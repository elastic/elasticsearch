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
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        return ShardTable.open(store, new TableLocation(ROOT, BlobPath.EMPTY.add("logs")), shard);
    }

    /** A merged file for the range, in the local directory, as the flusher produces it. */
    private LocalParquetFiles.LocalFile mergedFile(ShardTable table, long from, long to, boolean extraColumn) throws IOException {
        final LocalParquetFiles.LocalFile local = TestLocalFiles.write(directory, from, to, extraColumn);
        final MessageType union = ParquetMerger.unionSchema(directory, List.of(local));
        final MessageType withIds = ShardTable.withFieldIds(union, table.ensureColumns(union));
        final String name = "pqmerged_" + ShardTable.dataFileName(from, to);
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
        try (IndexInput in = directory.openInput(merged.name(), IOContext.DEFAULT)) {
            table.upload(
                merged.fromSeqNo(),
                merged.toSeqNo(),
                new java.io.BufferedInputStream(new InputStreamIndexInput(in, merged.length())),
                merged.length()
            );
        }
        final DataFile file = table.dataFile(
            merged.fromSeqNo(),
            merged.toSeqNo(),
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
        assertThat(files.get(1).location(), equalTo(table.dataLocation(100, 249)));
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

    public void testUploadingTheSameFileAgainReplacesItWithTheSameBytes() throws Exception {
        final ShardTable table = open(0);
        final LocalParquetFiles.LocalFile merged = mergedFile(table, 0, 9, false);

        uploadAndAppend(table, merged);
        // a retry after a crash uploads the bytes of the same local file again, and the table is not touched
        try (IndexInput in = directory.openInput(merged.name(), IOContext.DEFAULT)) {
            table.upload(0, 9, new java.io.BufferedInputStream(new InputStreamIndexInput(in, merged.length())), merged.length());
        }

        assertThat(table.uploadedFiles(), hasSize(1));
        assertThat(table.uploadedThroughSeqNo(), equalTo(9L));
    }
}
