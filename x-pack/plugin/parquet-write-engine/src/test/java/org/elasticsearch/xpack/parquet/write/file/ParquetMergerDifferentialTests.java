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
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The merge works on column readers and writers. The merge that it replaced built a {@code Group} for each row. That one stays here,
 * as the oracle: random files with random types, nulls, lists and columns that appear in later files must merge to the same rows.
 */
public class ParquetMergerDifferentialTests extends ESTestCase {

    private enum Kind {
        LONG(PrimitiveTypeName.INT64, null),
        INT(PrimitiveTypeName.INT32, null),
        FLOAT(PrimitiveTypeName.FLOAT, null),
        DOUBLE(PrimitiveTypeName.DOUBLE, null),
        BOOLEAN(PrimitiveTypeName.BOOLEAN, null),
        STRING(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        final PrimitiveTypeName type;
        final LogicalTypeAnnotation logical;

        Kind(PrimitiveTypeName type, LogicalTypeAnnotation logical) {
            this.type = type;
            this.logical = logical;
        }
    }

    private record Col(String name, Kind kind, boolean list) {}

    private Directory directory;

    @Before
    public void openDirectory() {
        directory = new ByteBuffersDirectory();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    public void testMatchesTheRowWiseMergeOnRandomFiles() throws Exception {
        final List<Col> pool = new ArrayList<>();
        final int columns = between(1, 14);
        for (int i = 0; i < columns; i++) {
            pool.add(new Col("c" + i, randomFrom(Kind.values()), randomBoolean()));
        }
        final int files = between(1, 5);
        final List<LocalParquetFiles.LocalFile> inputs = new ArrayList<>();
        int used = between(1, columns);
        long next = 0;
        for (int f = 0; f < files; f++) {
            // a later file may have more columns, as after a mapping update, and never fewer
            used = Math.min(columns, used + (rarely() ? 0 : between(0, 2)));
            final int rows = between(1, 3_000);
            inputs.add(writeFile(pool.subList(0, used), next, rows));
            next += rows;
        }
        final MessageType schema = ParquetMerger.unionSchema(directory, inputs);
        final CompressionCodecName codec = randomFrom(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD
        );
        final long rowGroupBytes = randomFrom(4_096L, 20_000L, 100_000L, 1L << 20);

        final ParquetMerger.Merged actual = ParquetMerger.merge(directory, inputs, schema, "pqmerged_actual.parquet", codec, rowGroupBytes);
        final List<ParquetMerger.Input> oracleInputs = new ArrayList<>();
        for (LocalParquetFiles.LocalFile file : inputs) {
            oracleInputs.add(new ParquetMerger.Input(file.name(), new IndexInputFile(directory, file.name())));
        }
        final ParquetMerger.Merged expected = RowWise.mergeInputs(
            directory,
            oracleInputs,
            schema,
            "pqmerged_expected.parquet",
            codec,
            rowGroupBytes
        );

        assertThat(actual.rows(), equalTo(expected.rows()));
        final Contents actualContents = read("pqmerged_actual.parquet");
        final Contents expectedContents = read("pqmerged_expected.parquet");
        assertThat(actualContents.schema(), equalTo(expectedContents.schema()));
        assertThat(actualContents.rows().size(), equalTo(expectedContents.rows().size()));
        for (int i = 0; i < actualContents.rows().size(); i++) {
            assertThat("row " + i, actualContents.rows().get(i).toString(), equalTo(expectedContents.rows().get(i).toString()));
        }
        if (actual.rows() > 5_000 && rowGroupBytes <= 20_000) {
            assertThat("small row groups were cut", actualContents.rowGroups(), greaterThan(1));
        }
    }

    // ---- the random files ----

    private LocalParquetFiles.LocalFile writeFile(List<Col> columns, long from, int rows) throws IOException {
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Col col : columns) {
            builder.addField(col.list() ? ParquetLists.listOf(col.name(), col.kind().type, col.kind().logical) : flat(col));
        }
        final MessageType schema = builder.named("doc");
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        final String name = LocalParquetFiles.name(from, from + rows - 1);
        try (IndexOutput out = directory.createOutput(name, IOContext.DEFAULT)) {
            try (
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BatchParquetWriter.IndexOutputFile(out))
                    .withConf(new PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .build()
            ) {
                for (int row = 0; row < rows; row++) {
                    final Group group = factory.newGroup();
                    for (Col col : columns) {
                        if (rarely() || randomIntBetween(0, 3) == 0) {
                            continue; // a null, or for a list no list at all
                        }
                        if (col.list()) {
                            final Group list = group.addGroup(col.name());
                            final int elements = between(0, 5); // 0 is an empty list
                            for (int e = 0; e < elements; e++) {
                                final Group item = list.addGroup(ParquetLists.LIST);
                                if (randomIntBetween(0, 5) != 0) { // else a null element
                                    add(item, ParquetLists.ELEMENT, col.kind());
                                }
                            }
                        } else {
                            add(group, col.name(), col.kind());
                        }
                    }
                    writer.write(group);
                }
            }
        }
        return new LocalParquetFiles.LocalFile(name, from, from + rows - 1, directory.fileLength(name));
    }

    private static Type flat(Col col) {
        final Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(col.kind().type);
        return (col.kind().logical == null ? builder : builder.as(col.kind().logical)).named(col.name());
    }

    private void add(Group group, String field, Kind kind) {
        switch (kind) {
            case LONG -> group.add(field, randomBoolean() ? randomLong() : (long) between(-5, 5));
            case INT -> group.add(field, randomBoolean() ? randomInt() : between(-5, 5));
            case FLOAT -> group.add(field, randomFloat());
            case DOUBLE -> group.add(field, randomDouble());
            case BOOLEAN -> group.add(field, randomBoolean());
            // few distinct values (a dictionary) or mostly unique ones (the writer falls back to plain)
            case STRING -> group.add(field, Binary.fromString(randomBoolean() ? "k" + between(0, 20) : randomAlphaOfLengthBetween(1, 60)));
        }
    }

    private record Contents(MessageType schema, List<Group> rows, int rowGroups) {}

    private Contents read(String name) throws IOException {
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
            return new Contents(schema, rows, rowGroups);
        }
    }

    /** The merge that built a Group for each row, as it was before the merge worked on columns. */
    private static final class RowWise {
        private RowWise() {}

        static ParquetMerger.Merged mergeInputs(
            Directory directory,
            List<ParquetMerger.Input> inputs,
            MessageType outputSchema,
            String outputName,
            CompressionCodecName codec,
            long rowGroupBytes
        ) throws IOException {
            final SimpleGroupFactory factory = new SimpleGroupFactory(outputSchema);
            final List<String> outputColumns = new ArrayList<>();
            for (Type field : outputSchema.getFields()) {
                outputColumns.add(field.getName());
            }
            long rows = 0;
            String tempName = null;
            boolean success = false;
            try (IndexOutput out = directory.createTempOutput(LocalParquetFiles.TEMP_PREFIX, "parquet", IOContext.DEFAULT)) {
                tempName = out.getName();
                try (
                    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BatchParquetWriter.IndexOutputFile(out))
                        .withConf(new PlainParquetConfiguration())
                        .withCodecFactory(new ParquetCodecs())
                        .withType(outputSchema)
                        .withCompressionCodec(codec)
                        .withRowGroupSize(rowGroupBytes)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .build()
                ) {
                    for (ParquetMerger.Input input : inputs) {
                        rows += copyRows(input, outputSchema, outputColumns, factory, writer);
                    }
                }
                success = true;
            } finally {
                if (success == false && tempName != null) {
                    try {
                        directory.deleteFile(tempName);
                    } catch (IOException e) {
                        // removed with the other temporary files when the engine starts again
                    }
                }
            }
            directory.sync(List.of(tempName));
            directory.rename(tempName, outputName);
            directory.syncMetaData();
            return new ParquetMerger.Merged(outputName, directory.fileLength(outputName), rows);
        }

        private static long copyRows(
            ParquetMerger.Input input,
            MessageType outputSchema,
            List<String> outputColumns,
            SimpleGroupFactory factory,
            ParquetWriter<Group> writer
        ) throws IOException {
            long rows = 0;
            try (
                ParquetFileReader reader = ParquetFileReader.open(
                    input.file(),
                    PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
                )
            ) {
                final MessageType inputSchema = reader.getFooter().getFileMetaData().getSchema();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(inputSchema);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(inputSchema));
                    for (long i = 0; i < pages.getRowCount(); i++) {
                        final Group in = recordReader.read();
                        final Group out = factory.newGroup();
                        for (String column : outputColumns) {
                            if (inputSchema.containsField(column) == false) {
                                continue; // an earlier file that lacks a column that was added later: a null
                            }
                            final int index = inputSchema.getFieldIndex(column);
                            if (in.getFieldRepetitionCount(index) == 0) {
                                continue; // a null, or a list with no value
                            }
                            final Type outputType = outputSchema.getType(column);
                            if (ParquetLists.isList(outputType)) {
                                copyList(in.getGroup(index, 0), out.addGroup(column), column, ParquetLists.elementOf(outputType));
                            } else {
                                copyValue(in, index, out, column, outputType.asPrimitiveType().getPrimitiveTypeName(), 0);
                            }
                        }
                        writer.write(out);
                        rows++;
                    }
                }
            }
            return rows;
        }

        /** The elements of a list column, with the null elements (a list item without an element). */
        private static void copyList(Group inList, Group outList, String column, PrimitiveType element) {
            final int items = inList.getFieldRepetitionCount(ParquetLists.LIST);
            for (int i = 0; i < items; i++) {
                final Group inItem = inList.getGroup(ParquetLists.LIST, i);
                final Group outItem = outList.addGroup(ParquetLists.LIST);
                if (inItem.getFieldRepetitionCount(ParquetLists.ELEMENT) > 0) {
                    copyValue(
                        inItem,
                        inItem.getType().getFieldIndex(ParquetLists.ELEMENT),
                        outItem,
                        ParquetLists.ELEMENT,
                        element.getPrimitiveTypeName(),
                        0
                    );
                }
            }
        }

        /** One value of a primitive field, at {@code position} of the repeated field {@code index} of {@code in}. */
        private static void copyValue(Group in, int index, Group out, String field, PrimitiveType.PrimitiveTypeName type, int position) {
            switch (type) {
                case INT32 -> out.add(field, in.getInteger(index, position));
                case INT64 -> out.add(field, in.getLong(index, position));
                case FLOAT -> out.add(field, in.getFloat(index, position));
                case DOUBLE -> out.add(field, in.getDouble(index, position));
                case BOOLEAN -> out.add(field, in.getBoolean(index, position));
                case BINARY -> out.add(field, in.getBinary(index, position));
                default -> throw new IllegalStateException("column [" + field + "] has the unsupported type [" + type + "]");
            }
        }
    }
}
