/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ParquetMergerTests extends ESTestCase {

    private Directory directory;

    @Before
    public void openDirectory() {
        directory = new ByteBuffersDirectory();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    private record Merged(List<Group> rows, int rowGroups, MessageType schema) {}

    private Merged read(String name) throws IOException {
        final List<Group> rows = new ArrayList<>();
        int rowGroups = 0;
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new IndexInputFile(directory, name),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                rowGroups++;
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    rows.add(recordReader.read());
                }
            }
            return new Merged(rows, rowGroups, schema);
        }
    }

    private void assertRowsMatchTheWriter(List<Group> rows, long from, long to) {
        assertThat(rows.size(), equalTo((int) (to - from + 1)));
        for (int i = 0; i < rows.size(); i++) {
            final long row = from + i;
            final Group group = rows.get(i);
            assertThat(group.getLong("count", 0), equalTo(row));
            assertThat(group.getLong("@timestamp", 0), equalTo(TestLocalFiles.BASE_TIMESTAMP + row * 1000));
            if (row % 7 == 0) {
                assertThat("null keyword of row " + row, group.getFieldRepetitionCount("host.name"), equalTo(0));
            } else {
                assertThat(group.getString("host.name", 0), equalTo("host-" + (row % 5)));
            }
        }
    }

    public void testMergesFilesInOrderWithLargerRowGroupsAndNulls() throws Exception {
        final List<LocalParquetFiles.LocalFile> inputs = List.of(
            TestLocalFiles.write(directory, 0, 999, false),
            TestLocalFiles.write(directory, 1000, 2499, false),
            TestLocalFiles.write(directory, 2500, 2999, false)
        );
        final MessageType schema = ParquetMerger.unionSchema(directory, inputs);
        final String outputName = "pqmerged_0_2999.parquet";

        final ParquetMerger.Merged merged = ParquetMerger.merge(
            directory,
            inputs,
            schema,
            outputName,
            CompressionCodecName.SNAPPY,
            1 << 16
        );

        assertThat(merged.rows(), equalTo(3000L));
        assertThat(merged.name(), equalTo(outputName));
        assertThat(merged.length(), equalTo(directory.fileLength(outputName)));
        final Merged read = read(outputName);
        assertRowsMatchTheWriter(read.rows(), 0, 2999);
        // many more rows than one small row group holds
        assertThat(read.rowGroups(), greaterThan(1));
        // the temporary file of the merge is gone
        for (String name : directory.listAll()) {
            assertFalse(name, name.startsWith(LocalParquetFiles.TEMP_PREFIX));
        }
    }

    public void testALaterFileMayHaveAColumnThatAnEarlierFileLacks() throws Exception {
        final List<LocalParquetFiles.LocalFile> inputs = List.of(
            TestLocalFiles.write(directory, 0, 49, false),
            TestLocalFiles.write(directory, 50, 99, true)
        );

        final MessageType schema = ParquetMerger.unionSchema(directory, inputs);
        assertThat(schema.getFieldCount(), equalTo(4));
        ParquetMerger.merge(directory, inputs, schema, "pqmerged_0_99.parquet", CompressionCodecName.UNCOMPRESSED, 1 << 20);

        final Merged read = read("pqmerged_0_99.parquet");
        assertThat(read.rows().size(), equalTo(100));
        for (int i = 0; i < 100; i++) {
            final Group group = read.rows().get(i);
            if (i < 50) {
                assertThat("an earlier file has no extra column: null", group.getFieldRepetitionCount("extra"), equalTo(0));
            } else {
                assertThat(group.getLong("extra", 0), equalTo(i * 2L));
            }
        }
    }

    public void testFieldIdsOfTheOutputSchemaAreWritten() throws Exception {
        final List<LocalParquetFiles.LocalFile> inputs = List.of(TestLocalFiles.write(directory, 0, 9, false));
        final MessageType union = ParquetMerger.unionSchema(directory, inputs);
        // what the table does: it gives each column its id
        final Types.MessageTypeBuilder withIds = Types.buildMessage();
        int id = 1;
        for (var field : union.getFields()) {
            withIds.addField(
                Types.optional(field.asPrimitiveType().getPrimitiveTypeName())
                    .as(field.getLogicalTypeAnnotation())
                    .id(id++)
                    .named(field.getName())
            );
        }
        final MessageType schema = withIds.named("doc");

        ParquetMerger.merge(directory, inputs, schema, "pqmerged_0_9.parquet", CompressionCodecName.UNCOMPRESSED, 1 << 20);

        final Merged read = read("pqmerged_0_9.parquet");
        assertThat(read.schema().getType("count").getId().intValue(), equalTo(1));
        assertThat(read.schema().getType("@timestamp").getId().intValue(), equalTo(2));
        assertThat(read.schema().getType("host.name").getId().intValue(), equalTo(3));
        assertRowsMatchTheWriter(read.rows(), 0, 9);
    }

    public void testAColumnWithTwoTypesIsRejected() throws Exception {
        final LocalParquetFiles.LocalFile first = TestLocalFiles.write(directory, 0, 9, false);
        // a second file whose "count" is a keyword
        final MessageType other = Types.buildMessage().addField(Types.optional(PrimitiveTypeName.BINARY).named("count")).named("doc");
        final String name = LocalParquetFiles.name(10, 19);
        try (var out = directory.createOutput(name, org.apache.lucene.store.IOContext.DEFAULT)) {
            try (
                var writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(new BatchParquetWriter.IndexOutputFile(out))
                    .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
                    .withCodecFactory(new PlainCompressionCodecFactory())
                    .withType(other)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()
            ) {
                final var group = new org.apache.parquet.example.data.simple.SimpleGroupFactory(other).newGroup();
                group.add("count", org.apache.parquet.io.api.Binary.fromString("ten"));
                writer.write(group);
            }
        }
        final LocalParquetFiles.LocalFile second = new LocalParquetFiles.LocalFile(name, 10, 19, directory.fileLength(name));

        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ParquetMerger.unionSchema(directory, List.of(first, second))
        );
        assertThat(e.getMessage(), containsString("column [count]"));
    }

    public void testZstdIsAvailableForTheMergedFile() throws Exception {
        final List<LocalParquetFiles.LocalFile> inputs = List.of(TestLocalFiles.write(directory, 0, 999, false));
        final MessageType schema = ParquetMerger.unionSchema(directory, inputs);

        ParquetMerger.merge(directory, inputs, schema, "pqmerged_0_999.parquet", CompressionCodecName.ZSTD, 1 << 20);

        assertRowsMatchTheWriter(read("pqmerged_0_999.parquet").rows(), 0, 999);
    }

    // ---- lists ----

    private static MessageType listSchema() {
        return org.apache.parquet.schema.Types.buildMessage()
            .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("n"))
            .addField(
                ParquetLists.listOf(
                    "tags",
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY,
                    org.apache.parquet.schema.LogicalTypeAnnotation.stringType()
                )
            )
            .addField(ParquetLists.listOf("nums", org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, null))
            .named("doc");
    }

    /** The expected tags of row {@code n}: null (no list), an empty list, one value, several with duplicates, or with null elements. */
    private static List<String> tagsOf(long n) {
        return switch ((int) (n % 6)) {
            case 0 -> null;
            case 1 -> List.of();
            case 2 -> List.of("only-" + n);
            case 3 -> List.of("b", "a", "b", "c-" + n);
            case 4 -> java.util.Arrays.asList("x", null, "y");
            case 5 -> java.util.Arrays.asList((String) null);
            default -> throw new AssertionError();
        };
    }

    private static List<Long> numsOf(long n) {
        return n % 4 == 0 ? null : n % 4 == 1 ? List.of(n, n, 7L, -1L) : List.of(n);
    }

    private LocalParquetFiles.LocalFile writeListFile(long from, long to) throws IOException {
        final MessageType schema = listSchema();
        final org.apache.parquet.example.data.simple.SimpleGroupFactory factory =
            new org.apache.parquet.example.data.simple.SimpleGroupFactory(schema);
        final String name = LocalParquetFiles.name(from, to);
        try (org.apache.lucene.store.IndexOutput out = directory.createOutput(name, org.apache.lucene.store.IOContext.DEFAULT)) {
            try (
                org.apache.parquet.hadoop.ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                    new BatchParquetWriter.IndexOutputFile(out)
                )
                    .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(schema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
            ) {
                for (long n = from; n <= to; n++) {
                    final Group row = factory.newGroup();
                    row.add("n", n);
                    final List<String> tags = tagsOf(n);
                    if (tags != null) {
                        final Group list = row.addGroup("tags");
                        for (String tag : tags) {
                            final Group item = list.addGroup(ParquetLists.LIST);
                            if (tag != null) {
                                item.add(ParquetLists.ELEMENT, tag);
                            }
                        }
                    }
                    final List<Long> nums = numsOf(n);
                    if (nums != null) {
                        final Group list = row.addGroup("nums");
                        for (long num : nums) {
                            list.addGroup(ParquetLists.LIST).add(ParquetLists.ELEMENT, num);
                        }
                    }
                    writer.write(row);
                }
            }
        }
        return new LocalParquetFiles.LocalFile(name, from, to, directory.fileLength(name));
    }

    private static List<String> tagsRead(Group row) {
        if (row.getFieldRepetitionCount("tags") == 0) {
            return null;
        }
        final Group list = row.getGroup("tags", 0);
        final List<String> tags = new ArrayList<>();
        for (int i = 0; i < list.getFieldRepetitionCount(ParquetLists.LIST); i++) {
            final Group item = list.getGroup(ParquetLists.LIST, i);
            tags.add(item.getFieldRepetitionCount(ParquetLists.ELEMENT) == 0 ? null : item.getString(ParquetLists.ELEMENT, 0));
        }
        return tags;
    }

    private static List<Long> numsRead(Group row) {
        if (row.getFieldRepetitionCount("nums") == 0) {
            return null;
        }
        final Group list = row.getGroup("nums", 0);
        final List<Long> nums = new ArrayList<>();
        for (int i = 0; i < list.getFieldRepetitionCount(ParquetLists.LIST); i++) {
            nums.add(list.getGroup(ParquetLists.LIST, i).getLong(ParquetLists.ELEMENT, 0));
        }
        return nums;
    }

    public void testListsKeepTheirOrderDuplicatesNullElementsAndTheDifferenceBetweenNullAndEmptyThroughAMerge() throws Exception {
        final List<LocalParquetFiles.LocalFile> inputs = List.of(writeListFile(0, 99), writeListFile(100, 249));
        final MessageType schema = ParquetMerger.unionSchema(directory, inputs);
        assertTrue(ParquetLists.isList(schema.getType("tags")));
        assertTrue(ParquetLists.isList(schema.getType("nums")));

        ParquetMerger.merge(directory, inputs, schema, "pqmerged_lists.parquet", CompressionCodecName.SNAPPY, 1 << 20);

        final Merged merged = read("pqmerged_lists.parquet");
        assertThat(merged.rows().size(), equalTo(250));
        for (Group row : merged.rows()) {
            final long n = row.getLong("n", 0);
            assertThat("tags of " + n, tagsRead(row), equalTo(tagsOf(n)));
            assertThat("nums of " + n, numsRead(row), equalTo(numsOf(n)));
        }
    }

    public void testAFieldIsAListInEveryFileOrInNone() throws Exception {
        final LocalParquetFiles.LocalFile lists = writeListFile(0, 9);
        // a file in which "tags" is a flat keyword column
        final MessageType flatSchema = org.apache.parquet.schema.Types.buildMessage()
            .addField(
                org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("tags")
            )
            .named("doc");
        final String flatName = LocalParquetFiles.name(10, 10);
        try (org.apache.lucene.store.IndexOutput out = directory.createOutput(flatName, org.apache.lucene.store.IOContext.DEFAULT)) {
            try (
                org.apache.parquet.hadoop.ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                    new BatchParquetWriter.IndexOutputFile(out)
                )
                    .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(flatSchema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
            ) {
                final Group row = new org.apache.parquet.example.data.simple.SimpleGroupFactory(flatSchema).newGroup();
                row.add("tags", "x");
                writer.write(row);
            }
        }
        final LocalParquetFiles.LocalFile flat = new LocalParquetFiles.LocalFile(flatName, 10, 10, directory.fileLength(flatName));

        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ParquetMerger.unionSchema(directory, List.of(lists, flat))
        );
        assertThat(e.getMessage(), containsString("tags"));
    }
}
