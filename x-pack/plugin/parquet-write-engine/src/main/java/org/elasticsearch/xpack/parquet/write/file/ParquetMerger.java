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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges the small local Parquet files that bulks wrote into one large file with row groups of the configured size and a real
 * compression codec. It decodes the values and encodes them again on purpose: a copy of the row groups would keep the tiny row groups of
 * the bulks, and those are what readers handle worst.
 *
 * <p>The values go from the column readers of the input to the column writers of the output with their repetition and definition levels,
 * and no row is ever assembled. An earlier version built a {@code Group} for each row and spent most of its time in that. This is safe
 * because a column has the same type, and so the same levels, in every file (see {@link #unionSchemaOf}). The row groups are cut like
 * the parquet-mr writer does it, by the size of the buffered data.
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
        long rows = 0;
        String tempName = null;
        boolean success = false;
        try (IndexOutput out = directory.createTempOutput(LocalParquetFiles.TEMP_PREFIX, "parquet", IOContext.DEFAULT)) {
            tempName = out.getName();
            try (
                ColumnarParquetWriter writer = ColumnarParquetWriter.forRows(
                    new BatchParquetWriter.IndexOutputFile(out),
                    outputSchema,
                    codec,
                    rowGroupBytes
                )
            ) {
                for (Input input : inputs) {
                    rows += copyRows(input, outputSchema, writer);
                }
                writer.finish();
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

    private static long copyRows(Input input, MessageType outputSchema, ColumnarParquetWriter writer) throws IOException {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(input.file(), readOptions())) {
            final FileMetaData metaData = reader.getFooter().getFileMetaData();
            final MessageType inputSchema = metaData.getSchema();
            final Map<String, ColumnDescriptor> inputColumns = new HashMap<>();
            for (ColumnDescriptor column : inputSchema.getColumns()) {
                inputColumns.put(String.join(".", column.getPath()), column);
            }
            final List<ColumnDescriptor> outputColumns = outputSchema.getColumns();
            // the converter is only a required argument of the read store: no value is ever converted
            final GroupConverter converter = new GroupRecordConverter(inputSchema).getRootConverter();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                final ColumnReadStoreImpl readStore = new ColumnReadStoreImpl(pages, converter, inputSchema, metaData.getCreatedBy());
                final ColumnCopier[] copiers = new ColumnCopier[outputColumns.size()];
                for (int i = 0; i < copiers.length; i++) {
                    final ColumnDescriptor target = outputColumns.get(i);
                    final ColumnDescriptor source = inputColumns.get(String.join(".", target.getPath()));
                    // a column that an earlier file lacks (added later by a mapping update) is a null in every row
                    copiers[i] = source == null
                        ? new ColumnCopier(writer, i, null, target, input.name())
                        : new ColumnCopier(writer, i, readStore.getColumnReader(source), target, input.name());
                }
                final long count = pages.getRowCount();
                for (long row = 0; row < count; row++) {
                    for (ColumnCopier copier : copiers) {
                        copier.copyRecord();
                    }
                    writer.endRecord();
                }
                rows += count;
            }
        }
        return rows;
    }

    /**
     * Copies the values of one column of the current row group of an input to the output, one record (a row) at a time. A record is the
     * values up to the next one with the repetition level 0, which is one value for a flat column and all the elements of a list.
     */
    private static final class ColumnCopier {
        private final ColumnarParquetWriter writer;
        private final int index;
        /** Null when the input file has no such column. */
        private final ColumnReader reader;
        private final int maxDefinitionLevel;
        private final PrimitiveType.PrimitiveTypeName type;
        private long remaining;

        ColumnCopier(ColumnarParquetWriter writer, int index, ColumnReader reader, ColumnDescriptor target, String inputName) {
            this.writer = writer;
            this.index = index;
            this.reader = reader;
            this.maxDefinitionLevel = target.getMaxDefinitionLevel();
            this.type = target.getPrimitiveType().getPrimitiveTypeName();
            if (reader != null) {
                final ColumnDescriptor source = reader.getDescriptor();
                if (source.getMaxDefinitionLevel() != target.getMaxDefinitionLevel()
                    || source.getMaxRepetitionLevel() != target.getMaxRepetitionLevel()
                    || source.getPrimitiveType().getPrimitiveTypeName() != type) {
                    // the levels of one column are the same in every file; copying them as they are would write a broken file otherwise
                    throw new IllegalStateException(
                        "column [" + String.join(".", target.getPath()) + "] has other levels or another type in [" + inputName + "]"
                    );
                }
                this.remaining = reader.getTotalValueCount();
            }
        }

        void copyRecord() {
            final ColumnWriter out = writer.columnWriter(index);
            if (reader == null) {
                out.writeNull(0, 0);
                return;
            }
            do {
                final int repetitionLevel = reader.getCurrentRepetitionLevel();
                final int definitionLevel = reader.getCurrentDefinitionLevel();
                if (definitionLevel < maxDefinitionLevel) {
                    out.writeNull(repetitionLevel, definitionLevel); // a null, an empty list or a list with no value
                } else {
                    switch (type) {
                        case INT32 -> out.write(reader.getInteger(), repetitionLevel, definitionLevel);
                        case INT64 -> out.write(reader.getLong(), repetitionLevel, definitionLevel);
                        case FLOAT -> out.write(reader.getFloat(), repetitionLevel, definitionLevel);
                        case DOUBLE -> out.write(reader.getDouble(), repetitionLevel, definitionLevel);
                        case BOOLEAN -> out.write(reader.getBoolean(), repetitionLevel, definitionLevel);
                        case BINARY -> out.write(reader.getBinary(), repetitionLevel, definitionLevel);
                        default -> throw new IllegalStateException(
                            "column [" + reader.getDescriptor() + "] has the unsupported type [" + type + "]"
                        );
                    }
                }
                reader.consume();
                remaining--;
            } while (remaining > 0 && reader.getCurrentRepetitionLevel() != 0);
        }
    }
}
