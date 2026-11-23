/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/** How the columns of a merge are split into groups for the threads. */
public class ParquetMergerGroupPlanTests extends ESTestCase {

    private Directory directory;

    @Before
    public void openDirectory() {
        directory = new ByteBuffersDirectory();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    /**
     * The columns are balanced by the cost of encoding them and not by their compressed size. A text column that compresses very well
     * is small in the file and costs a lot to encode (the statistics and the compression read all its bytes), and columns of random
     * numbers are the other way round. Balanced by compressed size, the text column would share a group with several number columns.
     */
    public void testAColumnThatIsCheapToStoreButExpensiveToEncodeGetsAGroupOfItsOwn() throws Exception {
        final int numbers = 7;
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("text"));
        for (int i = 0; i < numbers; i++) {
            builder.addField(Types.optional(PrimitiveTypeName.INT64).named("n" + i));
        }
        final MessageType schema = builder.named("doc");
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        for (int file = 0; file < 3; file++) {
            inputs.add(write(schema, file * 5_000L, 5_000, true));
        }

        final ParquetMerger.Plan plan = ParquetMerger.plan(inputs, schema, 2, 128L << 20);
        final List<MessageType> groups = plan.groups();

        assertThat(groups.size(), equalTo(Math.min(2 * ParquetMerger.GROUPS_PER_THREAD, numbers + 1)));
        assertThat("the threads take the most expensive group first", plan.dispatchOrder()[0], equalTo(0));
        assertThat(groups.get(0).getFields().stream().map(Type::getName).toList(), equalTo(List.of("text")));
        assertCoverTheSchemaInOrder(groups, schema);
    }

    public void testTheGroupsAreContiguousAndCoverTheSchema() throws Exception {
        final int columns = between(2, 30);
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < columns; i++) {
            builder.addField(Types.optional(PrimitiveTypeName.INT64).named("c" + i));
        }
        final MessageType schema = builder.named("doc");
        final List<ParquetMerger.Input> inputs = List.of(
            write(schema, 0, between(1, 500), false),
            write(schema, 1_000, between(1, 500), false)
        );
        final int parallelism = between(1, 40);

        final ParquetMerger.Plan plan = ParquetMerger.plan(inputs, schema, parallelism, 128L << 20);
        final List<MessageType> groups = plan.groups();

        assertThat(groups.size(), equalTo(parallelism == 1 ? 1 : Math.min(parallelism * ParquetMerger.GROUPS_PER_THREAD, columns)));
        final int[] order = plan.dispatchOrder().clone();
        Arrays.sort(order);
        for (int i = 0; i < order.length; i++) {
            assertThat("the dispatch order has each group once", order[i], equalTo(i));
        }
        assertThat(order.length, equalTo(groups.size()));
        assertCoverTheSchemaInOrder(groups, schema);
    }

    public void testAParallelismOfOneIsOneGroup() throws Exception {
        final MessageType schema = Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).named("a"))
            .addField(Types.optional(PrimitiveTypeName.INT64).named("b"))
            .named("doc");
        assertThat(ParquetMerger.plan(List.of(write(schema, 0, 10, false)), schema, 1, 128L << 20).groups(), equalTo(List.of(schema)));
    }

    /** The groups meet after each input row group, so an input with a very large one would make the row groups of the output coarse. */
    public void testAnInputWithAVeryLargeRowGroupIsMergedOnOneThread() throws Exception {
        final MessageType schema = Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).named("a"))
            .addField(Types.optional(PrimitiveTypeName.INT64).named("b"))
            .named("doc");
        final ParquetMerger.Input large = write(schema, 0, 100_001, false);
        assertThat(ParquetMerger.plan(List.of(large), schema, 4, 128L << 20).groups(), equalTo(List.of(schema)));
    }

    /**
     * The groups meet after an epoch, and a row group is cut only there, so an epoch has to be a small part of a row group. With files of
     * a few rows and a large row group an epoch is as many files as it can be, and the footers of that many files are held in memory.
     */
    public void testSmallFilesAndALargeRowGroupMakeEpochsOfManyInputs() throws Exception {
        final MessageType schema = twoColumns();
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        for (int file = 0; file < 5; file++) {
            inputs.add(write(schema, file * 10L, 10, false));
        }

        final ParquetMerger.Plan plan = ParquetMerger.plan(inputs, schema, 4, 128L << 20);

        assertThat(plan.inputsPerEpoch(), equalTo(ParquetMerger.MAX_INPUTS_PER_EPOCH));
    }

    /** A row group that is small for the rows of one file is cut after every file, as before the epochs. */
    public void testASmallRowGroupMakesEpochsOfOneInput() throws Exception {
        final MessageType schema = twoColumns();
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        for (int file = 0; file < 5; file++) {
            inputs.add(write(schema, file * 1_000L, 1_000, false));
        }

        assertThat(ParquetMerger.plan(inputs, schema, 4, 1_024).inputsPerEpoch(), equalTo(1));
        assertThat(ParquetMerger.plan(inputs, schema, 4, 100_000).inputsPerEpoch(), equalTo(1));
    }

    /** An epoch is more rows when the row group is larger, and never more than the limit. */
    public void testTheEpochGrowsWithTheRowGroup() throws Exception {
        final MessageType schema = twoColumns();
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        for (int file = 0; file < 5; file++) {
            inputs.add(write(schema, file * 100L, 100, false));
        }

        final int small = ParquetMerger.plan(inputs, schema, 4, 64L << 10).inputsPerEpoch();
        final int medium = ParquetMerger.plan(inputs, schema, 4, 1L << 20).inputsPerEpoch();
        final int large = ParquetMerger.plan(inputs, schema, 4, 1L << 30).inputsPerEpoch();

        assertTrue(small + " <= " + medium, small <= medium);
        assertTrue(medium + " <= " + large, medium <= large);
        assertThat(large, equalTo(ParquetMerger.MAX_INPUTS_PER_EPOCH));
        assertThat(small, greaterThanOrEqualTo(1));
    }

    private static MessageType twoColumns() {
        return Types.buildMessage()
            .addField(Types.optional(PrimitiveTypeName.INT64).named("a"))
            .addField(Types.optional(PrimitiveTypeName.INT64).named("b"))
            .named("doc");
    }

    private static void assertCoverTheSchemaInOrder(List<MessageType> groups, MessageType schema) {
        final List<Type> fields = new ArrayList<>();
        for (MessageType group : groups) {
            assertFalse("a group has a field", group.getFields().isEmpty());
            fields.addAll(group.getFields());
        }
        assertThat(fields, equalTo(schema.getFields()));
    }

    /** A file of one row group whose text column (if any) is one long string repeated, so that it compresses to almost nothing. */
    private ParquetMerger.Input write(MessageType schema, long from, int rows, boolean repeatedText) throws IOException {
        final String name = LocalParquetFiles.name(from, from + rows - 1);
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        final String text = "x".repeat(2_000);
        try (IndexOutput out = directory.createOutput(name, IOContext.DEFAULT)) {
            try (
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BatchParquetWriter.IndexOutputFile(out))
                    .withConf(new PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.ZSTD)
                    .withDictionaryEncoding(false)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .build()
            ) {
                for (int row = 0; row < rows; row++) {
                    final Group group = factory.newGroup();
                    for (Type field : schema.getFields()) {
                        if (field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
                            group.add(field.getName(), text);
                        } else {
                            group.add(field.getName(), randomLong());
                        }
                    }
                    writer.write(group);
                }
            }
        }
        return new ParquetMerger.Input(name, new IndexInputFile(directory, name));
    }
}
