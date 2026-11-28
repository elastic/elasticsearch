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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
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
        assertMatchesTheRowWiseMerge(null, 1);
    }

    /** The columns in groups, each encoded by its own thread: the same rows and schema, and every column chunk keeps its page indexes. */
    public void testTheParallelMergeMatchesTheRowWiseMergeOnRandomFiles() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(between(1, 4));
        try {
            // more groups than columns happens, and then there are fewer groups
            assertMatchesTheRowWiseMerge(executor, between(2, 8));
        } finally {
            terminate(executor);
        }
    }

    /** A task that the executor rejects runs on the thread of the merge, so a full pool never fails or stalls a flush. */
    public void testTheParallelMergeRunsATaskThatTheExecutorRejects() throws Exception {
        assertMatchesTheRowWiseMerge(command -> { throw new RejectedExecutionException("full"); }, between(2, 8));
    }

    /**
     * A failure of one group stops the merge with that failure, leaves no output behind, and returns only when no task runs any more:
     * the files of the merge are closed by the caller, and a task that still reads from them would read a closed file.
     */
    public void testAFailedGroupLeavesNoFileAndNoRunningTask() throws Exception {
        final List<Col> pool = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            pool.add(new Col("c" + i, randomFrom(Kind.values()), randomBoolean()));
        }
        final List<LocalParquetFiles.LocalFile> files = new ArrayList<>();
        for (int f = 0; f < 6; f++) {
            files.add(writeFile(pool, f * 100L, 100));
        }
        final MessageType schema = ParquetMerger.unionSchema(directory, files);
        final int failingOpen = between(3, 20);
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger running = new AtomicInteger();
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        for (LocalParquetFiles.LocalFile file : files) {
            final IndexInputFile real = new IndexInputFile(directory, file.name());
            inputs.add(new ParquetMerger.Input(file.name(), new InputFile() {
                @Override
                public long getLength() throws IOException {
                    return real.getLength();
                }

                @Override
                public SeekableInputStream newStream() throws IOException {
                    if (opens.incrementAndGet() == failingOpen) {
                        throw new IOException("injected");
                    }
                    running.incrementAndGet();
                    final SeekableInputStream stream = real.newStream();
                    return new DelegatingSeekableInputStream(stream) {
                        @Override
                        public void close() throws IOException {
                            running.decrementAndGet();
                            super.close();
                        }

                        @Override
                        public long getPos() throws IOException {
                            return stream.getPos();
                        }

                        @Override
                        public void seek(long newPos) throws IOException {
                            stream.seek(newPos);
                        }
                    };
                }
            }));
        }
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            final IOException e = expectThrows(
                IOException.class,
                () -> ParquetMerger.mergeInputs(
                    directory,
                    inputs,
                    schema,
                    "pqmerged_x.parquet",
                    CompressionCodecName.ZSTD,
                    1L << 20,
                    executor,
                    4
                )
            );
            assertThat(e.getMessage(), containsString("injected"));
            assertThat("no reader is left open", running.get(), equalTo(0));
            for (String name : directory.listAll()) {
                assertFalse(
                    "no temporary or merged file: " + name,
                    name.startsWith(LocalParquetFiles.TEMP_PREFIX) || name.startsWith("pqmerged_")
                );
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * The footers can be read on several threads, and the union is the same as the serial one and as the order in which the columns first
     * appear in the files: the files are many and small, as the local files of a flush are, and each has some of the columns in any order.
     */
    public void testTheUnionOfTheSchemasIsTheSameWhenTheFootersAreReadInParallel() throws Exception {
        final List<Col> pool = new ArrayList<>();
        final int columns = between(3, 12);
        for (int i = 0; i < columns; i++) {
            pool.add(new Col("c" + i, randomFrom(Kind.values()), randomBoolean()));
        }
        final List<LocalParquetFiles.LocalFile> files = new ArrayList<>();
        final List<String> expectedOrder = new ArrayList<>(); // the first appearance, computed here and not by the code under test
        for (int f = 0; f < between(130, 300); f++) {
            final List<Col> shuffled = new ArrayList<>(pool);
            Collections.shuffle(shuffled, random());
            final List<Col> columnsOfFile = shuffled.subList(0, between(1, columns));
            for (Col col : columnsOfFile) {
                if (expectedOrder.contains(col.name()) == false) {
                    expectedOrder.add(col.name());
                }
            }
            files.add(writeFile(columnsOfFile, f * 10L, 3));
        }
        final ExecutorService executor = Executors.newFixedThreadPool(between(1, 4));
        try {
            final MessageType serial = ParquetMerger.unionSchema(directory, files);

            final MessageType parallel = ParquetMerger.unionSchema(directory, files, executor, between(2, 8));

            assertThat(parallel, equalTo(serial));
            assertThat(parallel.getFields().stream().map(Type::getName).toList(), equalTo(expectedOrder));
        } finally {
            terminate(executor);
        }
    }

    /** A footer that cannot be read fails the union with that failure, and no reader is left open when it returns. */
    public void testAFooterThatFailsToOpenFailsTheParallelUnionAndLeavesNoReader() throws Exception {
        final List<Col> pool = List.of(new Col("a", Kind.LONG, false), new Col("b", Kind.STRING, false));
        final List<ParquetMerger.Input> inputs = new ArrayList<>();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger running = new AtomicInteger();
        final int failingOpen = between(5, 150);
        for (int f = 0; f < 200; f++) {
            final LocalParquetFiles.LocalFile file = writeFile(pool, f * 10L, 3);
            final IndexInputFile real = new IndexInputFile(directory, file.name());
            inputs.add(new ParquetMerger.Input(file.name(), new InputFile() {
                @Override
                public long getLength() throws IOException {
                    return real.getLength();
                }

                @Override
                public SeekableInputStream newStream() throws IOException {
                    if (opens.incrementAndGet() == failingOpen) {
                        throw new IOException("injected");
                    }
                    running.incrementAndGet();
                    final SeekableInputStream stream = real.newStream();
                    return new DelegatingSeekableInputStream(stream) {
                        @Override
                        public void close() throws IOException {
                            running.decrementAndGet();
                            super.close();
                        }

                        @Override
                        public long getPos() throws IOException {
                            return stream.getPos();
                        }

                        @Override
                        public void seek(long newPos) throws IOException {
                            stream.seek(newPos);
                        }
                    };
                }
            }));
        }
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            final IOException e = expectThrows(IOException.class, () -> ParquetMerger.unionSchemaOf(inputs, executor, 4));
            assertThat(e.getMessage(), containsString("injected"));
            assertThat("no reader is left open", running.get(), equalTo(0));
        } finally {
            terminate(executor);
        }
    }

    private static void terminate(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }

    private void assertMatchesTheRowWiseMerge(Executor executor, int parallelism) throws Exception {
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

        final ParquetMerger.Merged actual = executor == null
            ? ParquetMerger.merge(directory, inputs, schema, "pqmerged_actual.parquet", codec, rowGroupBytes)
            : ParquetMerger.merge(directory, inputs, schema, "pqmerged_actual.parquet", codec, rowGroupBytes, executor, parallelism);
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
        final int[] actualIndexes = pageIndexes("pqmerged_actual.parquet");
        final int[] expectedIndexes = pageIndexes("pqmerged_expected.parquet");
        if (expectedIndexes[0] == expectedIndexes[1]) {
            assertThat("every column chunk has its page indexes", actualIndexes[1], equalTo(actualIndexes[0]));
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

    /** The number of column chunks of a file, and the number of those that have a column index and an offset index. */
    private int[] pageIndexes(String name) throws IOException {
        int chunks = 0;
        int indexed = 0;
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new IndexInputFile(directory, name),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            final List<ColumnDescriptor> columns = reader.getFooter().getFileMetaData().getSchema().getColumns();
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                assertThat(block.getColumns().size(), equalTo(columns.size()));
                for (int i = 0; i < columns.size(); i++) {
                    final ColumnChunkMetaData chunk = block.getColumns().get(i);
                    // readers find a chunk by its path, so the order is not needed for the rows, but a file that is read from the
                    // start to the end needs the chunks in the order of the columns
                    assertArrayEquals(columns.get(i).getPath(), chunk.getPath().toArray());
                    chunks++;
                    if (reader.readColumnIndex(chunk) != null && reader.readOffsetIndex(chunk) != null) {
                        indexed++;
                    }
                }
            }
        }
        return new int[] { chunks, indexed };
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
