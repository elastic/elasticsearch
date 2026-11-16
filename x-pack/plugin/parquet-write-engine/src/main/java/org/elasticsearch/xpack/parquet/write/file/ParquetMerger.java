/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.ParquetReadOptions;
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
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges the small local Parquet files that bulks wrote into one large file with row groups of the configured size and a real
 * compression codec. It decodes the rows and encodes them again on purpose: a copy of the row groups would keep the tiny row groups of
 * the bulks, and those are what readers handle worst.
 *
 * <p>The files of one index come from one mapping that only grows, so a later file can have columns that an earlier file does not have.
 * The merged schema has all of them, and the rows of an earlier file get a null for the columns that it lacks.
 */
public final class ParquetMerger {

    /** The merged file: the name in the directory, its size and its number of rows. */
    public record Merged(String name, long length, long rows) {}

    /** A file to read the rows of: its name (for messages), and the file itself, which opens its own stream for each read. */
    public record Input(String name, InputFile file) {}

    private ParquetMerger() {}

    private static List<Input> inputsOf(Directory directory, List<LocalParquetFiles.LocalFile> files) {
        final List<Input> inputs = new ArrayList<>(files.size());
        for (LocalParquetFiles.LocalFile file : files) {
            inputs.add(new Input(file.name(), new IndexInputFile(directory, file.name())));
        }
        return inputs;
    }

    /**
     * The union of the schemas of the files, by column name, in the order that the columns first appear. Columns are flat. A column that
     * two files declare with different types is an error: the mapping never changes the type of a field.
     */
    public static MessageType unionSchema(Directory directory, List<LocalParquetFiles.LocalFile> files) throws IOException {
        return unionSchemaOf(inputsOf(directory, files));
    }

    /** {@link #unionSchema} for any files, for example the uploaded files that a compaction merges. */
    public static MessageType unionSchemaOf(List<Input> inputs) throws IOException {
        final Map<String, Type> columns = new LinkedHashMap<>();
        for (Input input : inputs) {
            final MessageType schema = readSchema(input.file());
            for (Type field : schema.getFields()) {
                final Type known = columns.putIfAbsent(field.getName(), field);
                if (known != null && sameTypeIgnoringIds(known, field) == false) {
                    throw new IllegalStateException(
                        "column ["
                            + field.getName()
                            + "] has the type ["
                            + known
                            + "] in one file and ["
                            + field
                            + "] in ["
                            + input.name()
                            + "]"
                    );
                }
            }
        }
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Type field : columns.values()) {
            builder.addField(field);
        }
        return builder.named("doc");
    }

    private static boolean sameTypeIgnoringIds(Type a, Type b) {
        if (ParquetLists.isList(a) || ParquetLists.isList(b)) {
            // a field is a list in every file or in none: the mapping decides, and it never changes
            return ParquetLists.isList(a)
                && ParquetLists.isList(b)
                && sameTypeIgnoringIds(ParquetLists.elementOf(a), ParquetLists.elementOf(b));
        }
        return a.isPrimitive()
            && b.isPrimitive()
            && a.asPrimitiveType().getPrimitiveTypeName() == b.asPrimitiveType().getPrimitiveTypeName()
            && java.util.Objects.equals(a.getLogicalTypeAnnotation(), b.getLogicalTypeAnnotation());
    }

    private static MessageType readSchema(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(file, readOptions())) {
            return reader.getFooter().getFileMetaData().getSchema();
        }
    }

    private static ParquetReadOptions readOptions() {
        return ParquetReadOptionsFactory.create(new ParquetCodecs());
    }

    /**
     * Writes the rows of {@code inputs}, in the order of the list, to a new file of the directory that is synced and renamed to
     * {@code outputName} when it is complete.
     *
     * @param outputSchema    the schema of the merged file, with the field ids of the table
     * @param codec           the compression of the merged file
     * @param rowGroupBytes   the size of the buffered data at which a row group is written
     */
    public static Merged merge(
        Directory directory,
        List<LocalParquetFiles.LocalFile> inputs,
        MessageType outputSchema,
        String outputName,
        CompressionCodecName codec,
        long rowGroupBytes
    ) throws IOException {
        return mergeInputs(directory, inputsOf(directory, inputs), outputSchema, outputName, codec, rowGroupBytes);
    }

    /** {@link #merge} for any files; the output is always a file of the directory, because the upload needs its length. */
    public static Merged mergeInputs(
        Directory directory,
        List<Input> inputs,
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
                for (Input input : inputs) {
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
        return new Merged(outputName, directory.fileLength(outputName), rows);
    }

    private static long copyRows(
        Input input,
        MessageType outputSchema,
        List<String> outputColumns,
        SimpleGroupFactory factory,
        ParquetWriter<Group> writer
    ) throws IOException {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(input.file(), readOptions())) {
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
