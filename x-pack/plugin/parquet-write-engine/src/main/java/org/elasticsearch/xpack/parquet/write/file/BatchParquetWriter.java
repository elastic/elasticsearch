/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.DictionaryColumn;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.OrdinalsTupleCursor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Writes the documents of one mapped batch as one Parquet file in the store directory of the shard. The mapper has already parsed the
 * values (dates, numbers, keywords), so this class reads each mapped column through the typed cursors of Lucene's column API and builds
 * no Lucene field and no Lucene document.
 *
 * <p>The schema comes from the mapping, never from the values of the batch, so the column of a field has the same type in every file.
 */
public final class BatchParquetWriter {

    /**
     * The field types that the engine can write. An index with another type is refused when it is created (see the validation of the
     * mapping in the settings provider), and a type that is added later fails the bulk that writes it.
     */
    public static final Set<String> SUPPORTED_FIELD_TYPES = Set.of(
        "long",
        "integer",
        "short",
        "byte",
        "date",
        "keyword",
        "double",
        "float",
        "boolean"
    );

    private BatchParquetWriter() {}

    /**
     * Writes the documents {@code d} of the batch for which {@code accepted[d]} is true, in the order of the batch, and returns the file
     * after it is durable. The sequence numbers {@code firstSeqNo} to {@code firstSeqNo + (accepted count) - 1} name the file.
     *
     * @param multiValued the full paths of the fields that are lists (see {@link MultiValuedFields})
     * @throws IllegalArgumentException if a mapped column has a type that the engine cannot write, or a document has more than one value
     *                                  for a field that is not a list
     */
    public static LocalParquetFiles.LocalFile write(
        Directory directory,
        ColumnBatch batch,
        MappingLookup lookup,
        boolean[] accepted,
        long firstSeqNo,
        CompressionCodecName codec,
        Set<String> multiValued
    ) throws IOException {
        int rows = 0;
        for (boolean a : accepted) {
            if (a) {
                rows++;
            }
        }
        if (rows == 0) {
            throw new IllegalArgumentException("a Parquet file needs at least one document");
        }
        final List<ColumnSource> sources = new ArrayList<>();
        for (Column column : batch.columns()) {
            final ColumnSource source = ColumnSource.of(column, lookup, multiValued);
            if (source != null) {
                sources.add(source);
            }
        }
        addColumnsWithoutValues(sources, lookup, multiValued);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("the mapping has no field that the Parquet write engine can write");
        }
        final Types.MessageTypeBuilder schemaBuilder = Types.buildMessage();
        for (ColumnSource source : sources) {
            schemaBuilder.addField(source.parquetField());
        }
        final MessageType schema = schemaBuilder.named("doc");

        final long lastSeqNo = firstSeqNo + rows - 1;
        final String finalName = LocalParquetFiles.name(firstSeqNo, lastSeqNo);
        String tempName = null;
        boolean success = false;
        try (IndexOutput out = directory.createTempOutput(LocalParquetFiles.TEMP_PREFIX, "parquet", IOContext.DEFAULT)) {
            tempName = out.getName();
            // The bulk is one row group, and each column is written on its own, straight from its cursor to the column writer of
            // parquet-mr.
            // A row group of a bulk is small, and the merge of the flush makes the row groups that readers want.
            try (
                ColumnarParquetWriter writer = new ColumnarParquetWriter(
                    new IndexOutputFile(out),
                    schema,
                    codec,
                    Long.MAX_VALUE,
                    ColumnarParquetWriter.columnProperties()
                )
            ) {
                for (int i = 0; i < sources.size(); i++) {
                    sources.get(i).writeColumn(writer.columnWriter(i), accepted);
                }
                writer.rowsWrittenByColumns(rows);
                writer.finish();
            }
            success = true;
        } finally {
            if (success == false && tempName != null) {
                deleteQuietly(directory, tempName);
            }
        }
        directory.sync(List.of(tempName));
        directory.rename(tempName, finalName);
        directory.syncMetaData();
        return new LocalParquetFiles.LocalFile(finalName, firstSeqNo, lastSeqNo, directory.fileLength(finalName));
    }

    /**
     * The batch has a column only for a field that at least one of its documents has a value for. The file must have a column for every
     * mapped field anyway: a reader of the table (the reader prototype) needs every file to have every column of the others, and the
     * schema of a file should not depend on which fields the documents of its bulk happened to have. A field without a value in the
     * batch becomes a column of nulls.
     */
    private static void addColumnsWithoutValues(List<ColumnSource> sources, MappingLookup lookup, Set<String> multiValued) {
        final Set<String> present = new HashSet<>();
        for (ColumnSource source : sources) {
            present.add(source.name);
        }
        for (Mapper mapper : lookup.fieldMappers()) {
            if (mapper instanceof FieldMapper field) {
                final String name = field.fullPath();
                if (name.startsWith("_") || present.contains(name)) {
                    continue;
                }
                final ColumnSource.TypeSpec spec = ColumnSource.typeSpec(field.fieldType().typeName());
                if (spec != null) {
                    sources.add(new NullSource(name, spec, multiValued.contains(name)));
                    present.add(name);
                }
            }
        }
    }

    private static void deleteQuietly(Directory directory, String name) {
        try {
            directory.deleteFile(name);
        } catch (IOException e) {
            // The file is removed with the other temporary files when the engine starts again.
        }
    }

    /**
     * One mapped column, written on its own. A flat column has at most one value for a document (a document with more is an error), and a
     * list column has all the values of a document in a row. A document that is not accepted is not written, but its values are read, to
     * move the cursor on.
     *
     * <p>The levels that a value gets: a flat optional column has the definition level 1 for a value and 0 for a null. A list (see
     * {@link ParquetLists}) has the definition level 3 for an element, 2 for a null element, 1 for an empty list and 0 for a null list,
     * and the repetition level 0 for the first element of a row and 1 for the others.
     */
    private abstract static class ColumnSource {
        /** The definition level of a value of a flat column, and of an element of a list. */
        static final int FLAT_VALUE = 1;
        static final int LIST_ELEMENT = 3;
        static final int LIST_NULL_ELEMENT = 2;

        final String name;

        ColumnSource(String name) {
            this.name = name;
        }

        abstract org.apache.parquet.schema.Type parquetField();

        /** Writes the column for the documents {@code d} of the batch with {@code accepted[d]}, in the order of the batch. */
        abstract void writeColumn(ColumnWriter writer, boolean[] accepted);

        static IllegalArgumentException moreThanOneValue(String name) {
            return new IllegalArgumentException("field [" + name + "] has more than one value in a document");
        }

        static ColumnSource of(Column column, MappingLookup lookup, Set<String> multiValuedFields) {
            final String name = column.name();
            if (name.startsWith("_")) {
                return null; // metadata fields: _id, _seq_no, _version, _source and the others
            }
            final MappedFieldType fieldType = lookup.getFieldType(name);
            if (fieldType == null || lookup.getMapper(name) instanceof FieldMapper == false) {
                return null; // a column that belongs to no field, such as the offsets of an array
            }
            final String type = fieldType.typeName();
            final boolean multiValued = multiValuedFields.contains(name);
            if (column instanceof LongColumn longColumn) {
                final TypeSpec spec = typeSpec(type);
                if (spec == null || spec.physical == PrimitiveType.PrimitiveTypeName.BINARY) {
                    throw unsupported(name, type);
                }
                if (spec.physical == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                    requireKind(name, type, longColumn, LongColumn.NumericKind.DOUBLE);
                } else if (spec.physical == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    requireKind(name, type, longColumn, LongColumn.NumericKind.FLOAT);
                }
                return multiValued
                    ? new LongListSource(name, longColumn, spec.physical, spec.logical)
                    : new LongSource(name, longColumn, spec.physical, spec.logical);
            }
            if (type.equals("keyword")) {
                if (column instanceof BinaryColumn binaryColumn) {
                    return multiValued ? new BinaryListSource(name, binaryColumn) : new BinarySource(name, binaryColumn);
                }
                if (column instanceof DictionaryColumn dictionaryColumn) {
                    if (multiValued) {
                        throw unsupported(name, "keyword with several values, as a dictionary column");
                    }
                    return new DictionarySource(name, dictionaryColumn);
                }
            }
            throw unsupported(name, type + " (column " + column.getClass().getSimpleName() + ")");
        }

        /** The Parquet type of a column of the mapped type. */
        record TypeSpec(PrimitiveType.PrimitiveTypeName physical, LogicalTypeAnnotation logical) {}

        /** The Parquet type that the engine writes for a field type, or null for a type that it does not write. */
        static TypeSpec typeSpec(String type) {
            return switch (type) {
                case "long" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.INT64, null);
                case "integer" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.INT32, null);
                case "short" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true));
                case "byte" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true));
                case "date" -> new TypeSpec(
                    PrimitiveType.PrimitiveTypeName.INT64,
                    LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
                );
                case "double" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.DOUBLE, null);
                case "float" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.FLOAT, null);
                case "boolean" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.BOOLEAN, null);
                case "keyword" -> new TypeSpec(PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
                default -> null;
            };
        }

        /** A float or a double arrives as a sortable long, and the kind of the column says which one; any other kind is a surprise. */
        private static void requireKind(String name, String type, LongColumn column, LongColumn.NumericKind expected) {
            if (column.numericKind() != expected) {
                throw unsupported(name, type + " (numeric kind " + column.numericKind() + ", expected " + expected + ")");
            }
        }

        private static IllegalArgumentException unsupported(String name, String type) {
            return new IllegalArgumentException("field [" + name + "] of type [" + type + "] is not supported by the Parquet write engine");
        }
    }

    private static final class LongSource extends ColumnSource {
        private final LongColumn column;
        private final PrimitiveType.PrimitiveTypeName physical;
        private final LogicalTypeAnnotation logical;

        LongSource(String name, LongColumn column, PrimitiveType.PrimitiveTypeName physical, LogicalTypeAnnotation logical) {
            super(name);
            this.column = column;
            this.physical = physical;
            this.logical = logical;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            final Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(physical);
            return (logical == null ? builder : builder.as(logical)).named(name);
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            final LongTupleCursor cursor = column.tuples();
            int next = cursor.nextDoc();
            for (int doc = 0; doc < accepted.length; doc++) {
                if (next == doc) {
                    if (accepted[doc]) {
                        writeNumber(writer, physical, cursor.longValue(), 0, FLAT_VALUE);
                    }
                    next = cursor.nextDoc();
                    if (next == doc) {
                        throw moreThanOneValue(name);
                    }
                } else {
                    assert next > doc : "column [" + name + "] is at [" + next + "] but the batch is at [" + doc + "]";
                    if (accepted[doc]) {
                        writer.writeNull(0, 0);
                    }
                }
            }
        }
    }

    /** Writes a value of a numeric column, as the column type needs it. */
    private static void writeNumber(
        ColumnWriter writer,
        PrimitiveType.PrimitiveTypeName physical,
        long value,
        int repetitionLevel,
        int definitionLevel
    ) {
        switch (physical) {
            case INT32 -> writer.write(Math.toIntExact(value), repetitionLevel, definitionLevel);
            case INT64 -> writer.write(value, repetitionLevel, definitionLevel);
            // a float and a double are in the sortable encoding of Lucene, which keeps the order of the numbers
            case FLOAT -> writer.write(NumericUtils.sortableIntToFloat(Math.toIntExact(value)), repetitionLevel, definitionLevel);
            case DOUBLE -> writer.write(NumericUtils.sortableLongToDouble(value), repetitionLevel, definitionLevel);
            case BOOLEAN -> writer.write(value != 0, repetitionLevel, definitionLevel);
            default -> throw new IllegalStateException("a column has the unexpected type [" + physical + "]");
        }
    }

    /** A multi-valued numeric field: all the values of a document, in the order of the source and with duplicates, are one list. */
    private static final class LongListSource extends ColumnSource {
        private final LongColumn column;
        private final PrimitiveType.PrimitiveTypeName physical;
        private final LogicalTypeAnnotation logical;

        LongListSource(String name, LongColumn column, PrimitiveType.PrimitiveTypeName physical, LogicalTypeAnnotation logical) {
            super(name);
            this.column = column;
            this.physical = physical;
            this.logical = logical;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return ParquetLists.listOf(name, physical, logical);
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            final LongTupleCursor cursor = column.tuples();
            int next = cursor.nextDoc();
            for (int doc = 0; doc < accepted.length; doc++) {
                if (next == doc) {
                    // the cursor has at least one value for this document, so the list is never empty
                    int repetitionLevel = 0;
                    while (next == doc) {
                        if (accepted[doc]) {
                            writeNumber(writer, physical, cursor.longValue(), repetitionLevel, LIST_ELEMENT);
                        }
                        repetitionLevel = 1;
                        next = cursor.nextDoc();
                    }
                } else {
                    assert next > doc : "column [" + name + "] is at [" + next + "] but the batch is at [" + doc + "]";
                    if (accepted[doc]) {
                        writer.writeNull(0, 0);
                    }
                }
            }
        }
    }

    /**
     * A multi-valued keyword field: the batch has one blob for each document in the framing of the columnar codec,
     * {@code [vint slots]([vint length+1][bytes])...}, in the order of the source and with duplicates; a slot of {@code [vint 0]} is a null
     * element. A document with no blob has no value (a missing field, {@code null} and {@code []} are the same), and its list is null.
     */
    private static final class BinaryListSource extends ColumnSource {
        private final BinaryColumn column;

        BinaryListSource(String name, BinaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return ParquetLists.listOf(name, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            final ObjectTupleCursor<BytesRef> cursor = column.tuples();
            int next = cursor.nextDoc();
            for (int doc = 0; doc < accepted.length; doc++) {
                if (next == doc) {
                    if (accepted[doc]) {
                        final BytesRef blob = cursor.value();
                        final ByteArrayDataInput in = new ByteArrayDataInput(blob.bytes, blob.offset, blob.length);
                        final int slots = in.readVInt();
                        if (slots > 0) {
                            for (int slot = 0; slot < slots; slot++) {
                                final int repetitionLevel = slot == 0 ? 0 : 1;
                                final int lengthPlusOne = in.readVInt();
                                if (lengthPlusOne > 0) {
                                    final int length = lengthPlusOne - 1;
                                    // the bytes are in the blob, which stays as it is while this document is written
                                    writer.write(
                                        Binary.fromReusedByteArray(blob.bytes, in.getPosition(), length),
                                        repetitionLevel,
                                        LIST_ELEMENT
                                    );
                                    in.skipBytes(length);
                                } else {
                                    writer.writeNull(repetitionLevel, LIST_NULL_ELEMENT);
                                }
                            }
                        } else {
                            writer.writeNull(0, 0);
                        }
                    }
                    next = cursor.nextDoc();
                } else {
                    assert next > doc : "column [" + name + "] is at [" + next + "] but the batch is at [" + doc + "]";
                    if (accepted[doc]) {
                        writer.writeNull(0, 0);
                    }
                }
            }
        }
    }

    /** A column of a mapped field that no document of the batch has a value for: every row is null. */
    private static final class NullSource extends ColumnSource {
        private final TypeSpec spec;
        private final boolean list;

        NullSource(String name, TypeSpec spec, boolean list) {
            super(name);
            this.spec = spec;
            this.list = list;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            if (list) {
                return ParquetLists.listOf(name, spec.physical(), spec.logical());
            }
            final Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(spec.physical());
            return (spec.logical() == null ? builder : builder.as(spec.logical())).named(name);
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            for (boolean accept : accepted) {
                if (accept) {
                    writer.writeNull(0, 0);
                }
            }
        }
    }

    private static final class BinarySource extends ColumnSource {
        private final BinaryColumn column;

        BinarySource(String name, BinaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            final ObjectTupleCursor<BytesRef> cursor = column.tuples();
            int next = cursor.nextDoc();
            for (int doc = 0; doc < accepted.length; doc++) {
                if (next == doc) {
                    if (accepted[doc]) {
                        writer.write(reused(cursor.value()), 0, FLAT_VALUE);
                    }
                    next = cursor.nextDoc();
                    if (next == doc) {
                        throw moreThanOneValue(name);
                    }
                } else {
                    assert next > doc : "column [" + name + "] is at [" + next + "] but the batch is at [" + doc + "]";
                    if (accepted[doc]) {
                        writer.writeNull(0, 0);
                    }
                }
            }
        }
    }

    private static final class DictionarySource extends ColumnSource {
        private final DictionaryColumn column;

        DictionarySource(String name, DictionaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        }

        @Override
        void writeColumn(ColumnWriter writer, boolean[] accepted) {
            final List<BytesRef> dictionary = column.dictionary();
            final OrdinalsTupleCursor cursor = column.tuples();
            int next = cursor.nextDoc();
            for (int doc = 0; doc < accepted.length; doc++) {
                if (next == doc) {
                    if (accepted[doc]) {
                        writer.write(reused(dictionary.get(cursor.ordValue())), 0, FLAT_VALUE);
                    }
                    next = cursor.nextDoc();
                    if (next == doc) {
                        throw moreThanOneValue(name);
                    }
                } else {
                    assert next > doc : "column [" + name + "] is at [" + next + "] but the batch is at [" + doc + "]";
                    if (accepted[doc]) {
                        writer.writeNull(0, 0);
                    }
                }
            }
        }
    }

    /**
     * The bytes of a value without a copy. The cursors reuse their buffers, so the binary is marked as reused: the writers of parquet-mr
     * copy a binary that they keep (the dictionary and the statistics) and write the others at once.
     */
    private static Binary reused(BytesRef bytes) {
        return Binary.fromReusedByteArray(bytes.bytes, bytes.offset, bytes.length);
    }

    /**
     * Parquet writes forward only and the offsets in its footer count from the start of the file, which is the start of the output.
     * The output is closed by the caller, not by Parquet.
     */
    record IndexOutputFile(IndexOutput out) implements OutputFile {
        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                @Override
                public long getPos() {
                    return out.getFilePointer();
                }

                @Override
                public void write(int b) throws IOException {
                    out.writeByte((byte) b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.writeBytes(b, off, len);
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
}
