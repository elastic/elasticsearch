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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
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
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        final long lastSeqNo = firstSeqNo + rows - 1;
        final String finalName = LocalParquetFiles.name(firstSeqNo, lastSeqNo);
        String tempName = null;
        boolean success = false;
        try (IndexOutput out = directory.createTempOutput(LocalParquetFiles.TEMP_PREFIX, "parquet", IOContext.DEFAULT)) {
            tempName = out.getName();
            try (
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new IndexOutputFile(out))
                    .withConf(new PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(schema)
                    .withCompressionCodec(codec)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .build()
            ) {
                for (int doc = 0; doc < accepted.length; doc++) {
                    final Group group = accepted[doc] ? factory.newGroup() : null;
                    for (ColumnSource source : sources) {
                        source.consume(doc, group);
                    }
                    if (group != null) {
                        writer.write(group);
                    }
                }
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
     * One mapped column read in document order. A flat column has at most one value for a document (a document with more is an error),
     * and a list column reads all the values of a document in one call of {@code read}.
     */
    private abstract static class ColumnSource {
        final String name;
        /** The document of the next value of the column. */
        int nextDoc = -1;
        /** The document that {@link #read} is reading: a list column reads all the values of that document. */
        int currentDoc = -1;

        ColumnSource(String name) {
            this.name = name;
        }

        abstract org.apache.parquet.schema.Type parquetField();

        /** Moves the cursor to its first value. */
        abstract void start();

        /** Reads the value of the current document into the group (which is null for a document that is not written) and moves on. */
        abstract void read(Group group);

        final void consume(int doc, Group group) {
            if (nextDoc == -1) {
                start();
            }
            if (nextDoc == doc) {
                currentDoc = doc;
                read(group);
                final int following = nextDoc;
                if (following == doc) {
                    throw new IllegalArgumentException("field [" + name + "] has more than one value in a document");
                }
            } else {
                assert nextDoc > doc : "column [" + name + "] is at [" + nextDoc + "] but the batch is at [" + doc + "]";
            }
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
        private LongTupleCursor cursor;

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
        void start() {
            cursor = column.tuples();
            nextDoc = cursor.nextDoc();
        }

        @Override
        void read(Group group) {
            if (group != null) {
                addNumber(group, name, physical, cursor.longValue());
            }
            nextDoc = cursor.nextDoc();
        }
    }

    /** Adds a value of a numeric column to a group, as the column type needs it. */
    private static void addNumber(Group group, String field, PrimitiveType.PrimitiveTypeName physical, long value) {
        switch (physical) {
            case INT32 -> group.add(field, Math.toIntExact(value));
            case INT64 -> group.add(field, value);
            // a float and a double are in the sortable encoding of Lucene, which keeps the order of the numbers
            case FLOAT -> group.add(field, NumericUtils.sortableIntToFloat(Math.toIntExact(value)));
            case DOUBLE -> group.add(field, NumericUtils.sortableLongToDouble(value));
            case BOOLEAN -> group.add(field, value != 0);
            default -> throw new IllegalStateException("column [" + field + "] has the unexpected type [" + physical + "]");
        }
    }

    /** A multi-valued numeric field: all the values of a document, in the order of the source and with duplicates, are one list. */
    private static final class LongListSource extends ColumnSource {
        private final LongColumn column;
        private final PrimitiveType.PrimitiveTypeName physical;
        private final LogicalTypeAnnotation logical;
        private LongTupleCursor cursor;

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
        void start() {
            cursor = column.tuples();
            nextDoc = cursor.nextDoc();
        }

        @Override
        void read(Group group) {
            final Group list = group == null ? null : group.addGroup(name);
            while (nextDoc == currentDoc) {
                if (list != null) {
                    addNumber(list.addGroup(ParquetLists.LIST), ParquetLists.ELEMENT, physical, cursor.longValue());
                }
                nextDoc = cursor.nextDoc();
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
        private ObjectTupleCursor<BytesRef> cursor;

        BinaryListSource(String name, BinaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return ParquetLists.listOf(name, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        }

        @Override
        void start() {
            cursor = column.tuples();
            nextDoc = cursor.nextDoc();
        }

        @Override
        void read(Group group) {
            if (group != null) {
                final BytesRef blob = cursor.value();
                final ByteArrayDataInput in = new ByteArrayDataInput(blob.bytes, blob.offset, blob.length);
                final int slots = in.readVInt();
                if (slots > 0) {
                    final Group list = group.addGroup(name);
                    for (int slot = 0; slot < slots; slot++) {
                        final int lengthPlusOne = in.readVInt();
                        final Group item = list.addGroup(ParquetLists.LIST);
                        if (lengthPlusOne > 0) {
                            final byte[] value = new byte[lengthPlusOne - 1];
                            in.readBytes(value, 0, value.length);
                            item.add(ParquetLists.ELEMENT, Binary.fromConstantByteArray(value));
                        }
                    }
                }
            }
            nextDoc = cursor.nextDoc();
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
        void start() {
            nextDoc = Integer.MAX_VALUE; // no value in any document
        }

        @Override
        void read(Group group) {
            throw new IllegalStateException("column [" + name + "] has no values");
        }
    }

    private static final class BinarySource extends ColumnSource {
        private final BinaryColumn column;
        private ObjectTupleCursor<BytesRef> cursor;

        BinarySource(String name, BinaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        }

        @Override
        void start() {
            cursor = column.tuples();
            nextDoc = cursor.nextDoc();
        }

        @Override
        void read(Group group) {
            if (group != null) {
                group.add(name, toBinary(cursor.value()));
            }
            nextDoc = cursor.nextDoc();
        }
    }

    private static final class DictionarySource extends ColumnSource {
        private final DictionaryColumn column;
        private OrdinalsTupleCursor cursor;
        private List<BytesRef> dictionary;

        DictionarySource(String name, DictionaryColumn column) {
            super(name);
            this.column = column;
        }

        @Override
        org.apache.parquet.schema.Type parquetField() {
            return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
        }

        @Override
        void start() {
            dictionary = column.dictionary();
            cursor = column.tuples();
            nextDoc = cursor.nextDoc();
        }

        @Override
        void read(Group group) {
            if (group != null) {
                group.add(name, toBinary(dictionary.get(cursor.ordValue())));
            }
            nextDoc = cursor.nextDoc();
        }
    }

    /** A copy, because the cursors reuse their buffers. */
    private static Binary toBinary(BytesRef bytes) {
        final byte[] copy = new byte[bytes.length];
        System.arraycopy(bytes.bytes, bytes.offset, copy, 0, bytes.length);
        return Binary.fromConstantByteArray(copy);
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
