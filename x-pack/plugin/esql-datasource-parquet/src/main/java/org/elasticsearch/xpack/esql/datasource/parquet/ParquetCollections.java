/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ChildRelationColumns;
import org.elasticsearch.xpack.esql.datasources.spi.CollectionSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parquet's collections: a {@code LIST} whose element is a group. A {@code LIST} of primitives is not one: it is already a
 * multivalued column. A {@code MAP} is not one either.
 * <p>
 * The decode turns each leaf's repetition and definition levels into the columnar form of {@link ChildRelationColumns}:
 * offsets per row and one block per leaf positioned by element.
 */
final class ParquetCollections {
    private ParquetCollections() {}

    /** The collections of {@code schema}, each with its leaves sorted by name, which is the carrier's leaf order. */
    static List<CollectionSchema> describe(MessageType schema) {
        List<CollectionSchema> out = new ArrayList<>();
        for (Type field : schema.getFields()) {
            describe(field, field.getName(), out);
        }
        return out;
    }

    private static void describe(Type field, String path, List<CollectionSchema> out) {
        if (field.isPrimitive()) {
            return;
        }
        GroupType group = field.asGroupType();
        GroupType element = element(group);
        if (element != null) {
            out.add(collection(path, element));
            return;
        }
        if (group.getLogicalTypeAnnotation() == null) {
            for (Type child : group.getFields()) {
                describe(child, path + "." + child.getName(), out);
            }
        }
    }

    /** The element group of a collection, or {@code null} when {@code group} is not one. */
    @Nullable
    private static GroupType element(GroupType group) {
        LogicalTypeAnnotation logical = group.getLogicalTypeAnnotation();
        if (group.getFieldCount() != 1 || group.getType(0).isRepetition(Type.Repetition.REPEATED) == false) {
            return null;
        }
        Type repeated = group.getType(0);
        if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            Type elementType = ParquetFormatReader.isListElementType(repeated, group.getName())
                ? repeated
                : repeated.asGroupType().getType(0);
            return elementType.isPrimitive() ? null : elementType.asGroupType();
        }
        return null;
    }

    private static CollectionSchema collection(String path, GroupType element) {
        List<CollectionSchema.Leaf> leaves = new ArrayList<>();
        String[] unsupported = new String[1];
        leaves(element, "", path, leaves, unsupported);
        leaves.sort(Comparator.comparing(CollectionSchema.Leaf::name));
        return new CollectionSchema(path, leaves, unsupported[0]);
    }

    /**
     * The element's leaves. A structure's fields are further leaves of the same element. A collection inside the element with
     * one leaf is a multivalued leaf; with more, its values would arrive unpaired, so the whole collection is refused.
     */
    private static void leaves(GroupType group, String prefix, String path, List<CollectionSchema.Leaf> out, String[] unsupported) {
        for (Type child : group.getFields()) {
            String name = prefix + child.getName();
            if (child.isPrimitive()) {
                DataType type = leafType(child.asPrimitiveType());
                if (type != null) {
                    out.add(new CollectionSchema.Leaf(name, type));
                }
                continue;
            }
            GroupType childGroup = child.asGroupType();
            LogicalTypeAnnotation logical = childGroup.getLogicalTypeAnnotation();
            if (logical instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
                continue;
            }
            if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                List<PrimitiveType> inner = new ArrayList<>();
                primitives(childGroup, inner);
                if (inner.size() == 1) {
                    DataType type = leafType(inner.getFirst());
                    if (type != null) {
                        out.add(new CollectionSchema.Leaf(name, type));
                    }
                } else if (unsupported[0] == null) {
                    unsupported[0] = "["
                        + path
                        + "."
                        + name
                        + "] is a collection with more than one field inside each element of ["
                        + path
                        + "], which cannot be read yet";
                }
                continue;
            }
            leaves(childGroup, name + ".", path, out, unsupported);
        }
    }

    private static void primitives(Type type, List<PrimitiveType> out) {
        if (type.isPrimitive()) {
            out.add(type.asPrimitiveType());
            return;
        }
        for (Type child : type.asGroupType().getFields()) {
            primitives(child, out);
        }
    }

    /** The type a leaf reads as, or {@code null} for a physical type the decode does not convert yet. */
    @Nullable
    static DataType leafType(PrimitiveType primitive) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                }
                if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation i && i.isSigned() == false) {
                    yield DataType.LONG;
                }
                if (logical == null || logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                    yield DataType.INTEGER;
                }
                yield null;
            }
            case INT64 -> {
                if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                    yield ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS ? DataType.DATETIME : DataType.DATE_NANOS;
                }
                if (logical == null || logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation i && i.isSigned()) {
                    yield DataType.LONG;
                }
                yield null;
            }
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY -> logical == null
                || logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation ? DataType.KEYWORD : null;
            case INT96, FIXED_LEN_BYTE_ARRAY -> null;
        };
    }

    /**
     * Reads one collection column of a row group into its carrier. Built per read from the projected schema, then bound to each
     * row group's column readers.
     */
    static final class Decoder {
        private final String name;
        private final Leaf[] leaves;

        private Decoder(String name, Leaf[] leaves) {
            this.name = name;
            this.leaves = leaves;
        }

        /**
         * The decoder for the collection at {@code path} of {@code schema}, carrying {@code leafNames} in that order, or
         * {@code null} when the schema holds no readable collection there. A named leaf this file lacks reads as null.
         */
        @Nullable
        static Decoder of(MessageType schema, String path, List<CollectionSchema.Leaf> wanted) {
            CollectionSchema collection = describe(schema).stream().filter(c -> c.path().equals(path)).findFirst().orElse(null);
            if (collection == null) {
                // A file without the collection has no elements for it, as a file without a column has nulls for it
                return null;
            }
            if (collection.unsupported() != null) {
                throw new IllegalArgumentException(collection.unsupported());
            }
            String[] parts = path.split("\\.");
            Type node = schema;
            for (String part : parts) {
                node = node.asGroupType().getType(part);
            }
            GroupType group = node.asGroupType();
            GroupType element = element(group);
            String[] repeatedPath = Arrays.copyOf(parts, parts.length + 1);
            repeatedPath[parts.length] = group.getType(0).getName();
            int collectionRep = schema.getMaxRepetitionLevel(repeatedPath);
            int existsDef = schema.getMaxDefinitionLevel(repeatedPath);
            int elementDef = existsDef;
            String[] elementPath = repeatedPath;
            if (element != group.getType(0)) {
                elementPath = Arrays.copyOf(repeatedPath, repeatedPath.length + 1);
                elementPath[repeatedPath.length] = element.getName();
                elementDef = schema.getMaxDefinitionLevel(elementPath);
            }
            Leaf[] leaves = new Leaf[wanted.size()];
            for (int l = 0; l < leaves.length; l++) {
                String name = wanted.get(l).name();
                CollectionSchema.Leaf leaf = collection.leaves().stream().filter(c -> c.name().equals(name)).findFirst().orElse(null);
                if (leaf != null) {
                    // The carrier holds each leaf in the dataset's type, which the operators decode it as
                    if (leaf.type() != wanted.get(l).type()) {
                        throw new IllegalArgumentException(
                            "["
                                + path
                                + "."
                                + name
                                + "] is ["
                                + leaf.type().typeName()
                                + "] in this file but ["
                                + wanted.get(l).type().typeName()
                                + "] in the dataset, and an element's fields cannot be converted yet"
                        );
                    }
                    leaves[l] = new Leaf(leaf.type(), descriptor(schema, elementPath, name), collectionRep, existsDef, elementDef);
                }
            }
            return new Decoder(path, leaves);
        }

        /** The descriptor of the leaf under the element whose name, relative to the element, is {@code relative}. */
        private static ColumnDescriptor descriptor(MessageType schema, String[] elementPath, String relative) {
            String[] relativeParts = relative.split("\\.");
            for (ColumnDescriptor column : schema.getColumns()) {
                String[] columnPath = column.getPath();
                if (columnPath.length < elementPath.length + relativeParts.length
                    || Arrays.equals(columnPath, 0, elementPath.length, elementPath, 0, elementPath.length) == false) {
                    continue;
                }
                // A leaf that is itself a list holds its value further down; match the named groups only
                int at = elementPath.length;
                boolean matches = true;
                for (String part : relativeParts) {
                    if (at >= columnPath.length || columnPath[at].equals(part) == false) {
                        matches = false;
                        break;
                    }
                    at++;
                }
                if (matches) {
                    return column;
                }
            }
            throw new IllegalStateException("no Parquet column for collection leaf [" + relative + "]");
        }

        String name() {
            return name;
        }

        /** Binds the readers of a new row group. */
        void bind(ColumnReadStore store) {
            for (Leaf leaf : leaves) {
                if (leaf != null) {
                    leaf.bind(store.getColumnReader(leaf.descriptor));
                }
            }
        }

        /**
         * Reads {@code rows} rows into a carrier. A null element is dropped, as a null element of a list of primitives is, and
         * reported on {@code nullElements}.
         */
        BytesRefBlock read(int rows, BlockFactory blockFactory, Runnable nullElements) {
            Block[] blocks = new Block[leaves.length];
            int[] offsets = null;
            try {
                for (int l = 0; l < leaves.length; l++) {
                    if (leaves[l] == null) {
                        continue;
                    }
                    int[] leafOffsets = new int[rows + 1];
                    blocks[l] = leaves[l].read(rows, leafOffsets, blockFactory, nullElements);
                    if (offsets == null) {
                        offsets = leafOffsets;
                    } else if (Arrays.equals(offsets, leafOffsets) == false) {
                        throw new IllegalStateException("the leaves of collection [" + name + "] disagree on its elements");
                    }
                }
                if (offsets == null) {
                    offsets = new int[rows + 1];
                }
                for (int l = 0; l < leaves.length; l++) {
                    if (blocks[l] == null) {
                        blocks[l] = blockFactory.newConstantNullBlock(offsets[rows]);
                    }
                }
                return ChildRelationColumns.encode(blockFactory, rows, offsets, blocks);
            } finally {
                Releasables.closeExpectNoException(blocks);
            }
        }

        void skip(int rows) {
            for (Leaf leaf : leaves) {
                if (leaf != null) {
                    leaf.skip(rows);
                }
            }
        }
    }

    /** One leaf's reader, which knows from the levels where the collection's elements start and end. */
    private static final class Leaf {
        private final DataType type;
        private final ColumnDescriptor descriptor;
        /** The repetition level of a new element of the collection. */
        private final int collectionRep;
        /** The definition level at which the collection has an element at all. */
        private final int existsDef;
        /** The definition level at which that element is not null. */
        private final int elementDef;
        private final int maxDef;
        private ColumnReader reader;
        private long total;
        private long consumed;

        Leaf(DataType type, ColumnDescriptor descriptor, int collectionRep, int existsDef, int elementDef) {
            this.type = type;
            this.descriptor = descriptor;
            this.collectionRep = collectionRep;
            this.existsDef = existsDef;
            this.elementDef = elementDef;
            this.maxDef = descriptor.getMaxDefinitionLevel();
        }

        Block read(int rows, int[] offsets, BlockFactory blockFactory, Runnable nullElements) {
            Block.Builder builder = builder(blockFactory, rows);
            try {
                List<Object> values = new ArrayList<>();
                int elements = 0;
                boolean open = false;
                for (int row = 0; row < rows; row++) {
                    offsets[row] = elements;
                    boolean first = true;
                    while (first || hasNext() && reader.getCurrentRepetitionLevel() > 0) {
                        first = false;
                        int rep = reader.getCurrentRepetitionLevel();
                        int def = reader.getCurrentDefinitionLevel();
                        if (def < existsDef) {
                            // A null or empty collection: no elements
                            next();
                            continue;
                        }
                        if (rep <= collectionRep) {
                            if (open) {
                                append(builder, values);
                            }
                            if (def < elementDef) {
                                nullElements.run();
                                open = false;
                                next();
                                continue;
                            }
                            open = true;
                            elements++;
                        }
                        if (open && def == maxDef) {
                            values.add(value());
                            reader.consume();
                            consumed++;
                        } else {
                            next();
                        }
                    }
                    if (open) {
                        append(builder, values);
                        open = false;
                    }
                }
                offsets[rows] = elements;
                return builder.build();
            } finally {
                builder.close();
            }
        }

        void skip(int rows) {
            for (int row = 0; row < rows; row++) {
                boolean first = true;
                while (first || hasNext() && reader.getCurrentRepetitionLevel() > 0) {
                    first = false;
                    next();
                }
            }
        }

        void bind(ColumnReader reader) {
            this.reader = reader;
            this.total = reader.getTotalValueCount();
            this.consumed = 0;
        }

        private boolean hasNext() {
            return consumed < total;
        }

        /** Moves past the current level pair, and past its value when it has one. */
        private void next() {
            if (reader.getCurrentDefinitionLevel() == maxDef) {
                reader.skip();
            }
            reader.consume();
            consumed++;
        }

        private Block.Builder builder(BlockFactory blockFactory, int rows) {
            return switch (type) {
                case BOOLEAN -> blockFactory.newBooleanBlockBuilder(rows);
                case INTEGER -> blockFactory.newIntBlockBuilder(rows);
                case LONG, DATETIME, DATE_NANOS -> blockFactory.newLongBlockBuilder(rows);
                case DOUBLE -> blockFactory.newDoubleBlockBuilder(rows);
                case KEYWORD -> blockFactory.newBytesRefBlockBuilder(rows);
                default -> throw new IllegalStateException("unsupported collection leaf type [" + type + "]");
            };
        }

        private Object value() {
            PrimitiveType primitive = descriptor.getPrimitiveType();
            LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
            return switch (primitive.getPrimitiveTypeName()) {
                case BOOLEAN -> reader.getBoolean();
                case INT32 -> {
                    int v = reader.getInteger();
                    if (type == DataType.DATETIME) {
                        yield TimeUnit.DAYS.toMillis(v);
                    }
                    yield type == DataType.LONG ? Integer.toUnsignedLong(v) : (Object) v;
                }
                case INT64 -> {
                    long v = reader.getLong();
                    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts
                        && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
                        yield v * 1000L;
                    }
                    yield v;
                }
                case FLOAT -> (double) reader.getFloat();
                case DOUBLE -> reader.getDouble();
                case BINARY -> {
                    Binary binary = reader.getBinary();
                    yield new BytesRef(binary.getBytes());
                }
                case INT96, FIXED_LEN_BYTE_ARRAY -> throw new IllegalStateException("unsupported collection leaf [" + primitive + "]");
            };
        }

        /** Appends the element's values, collected so an element with none is a null rather than an empty position. */
        private static void append(Block.Builder builder, List<Object> values) {
            if (values.isEmpty()) {
                builder.appendNull();
                return;
            }
            if (values.size() > 1) {
                builder.beginPositionEntry();
            }
            for (Object value : values) {
                switch (builder) {
                    case BooleanBlock.Builder b -> b.appendBoolean((Boolean) value);
                    case IntBlock.Builder b -> b.appendInt((Integer) value);
                    case LongBlock.Builder b -> b.appendLong((Long) value);
                    case DoubleBlock.Builder b -> b.appendDouble((Double) value);
                    case BytesRefBlock.Builder b -> b.appendBytesRef((BytesRef) value);
                    default -> throw new IllegalStateException("unsupported builder [" + builder + "]");
                }
            }
            if (values.size() > 1) {
                builder.endPositionEntry();
            }
            values.clear();
        }
    }
}
