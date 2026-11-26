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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ChildRelationRead;
import org.elasticsearch.xpack.esql.datasources.spi.ChildWindows;
import org.elasticsearch.xpack.esql.datasources.spi.CollectionSchema;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The child relations one Parquet read decodes beside its rows: for each requested collection, the requested element
 * leaves, read through the same column-read store as the row's own repeated columns. That store holds one stateful
 * page reader per column, so each collection is decoded exactly once per scan, beside the rows rather than in a
 * second pass that would need a reader of its own.
 * <p>
 * The decode runs ahead of the row's columns for each batch and may end the batch early: once the batch's elements
 * reach {@link #BATCH_ELEMENT_BUDGET} it stops at that row, and the iterator reads the other columns for that many rows
 * only. So the elements held for one parent page are bounded by the budget plus one parent's elements; a single parent
 * larger than the budget is held whole, charged to the breaker like every other block.
 * <p>
 * What an element is comes from {@link ParquetCollections.Shape}, the levels the carrier decode uses too, and the
 * element semantics match it: a null element is dropped and announced, an empty or null list has no elements, a leaf
 * the file lacks is null on every element, and a list inside the element is that leaf's multivalue.
 */
final class ParquetChildRelations {

    /** The elements one batch decodes before it ends at a row boundary. */
    static final int BATCH_ELEMENT_BUDGET = 64 * ChildWindows.MAX_ELEMENTS_PER_PAGE;

    private final Relation[] relations;
    /** The windows of the parent page last returned, parallel to {@link #relations}; {@code null} before the first. */
    @Nullable
    private ChildWindows[] current;

    private ParquetChildRelations(Relation[] relations) {
        this.relations = relations;
    }

    /**
     * The relations {@code reads} names, resolved against {@code projectedSchema}, the schema the iterator reads, which
     * {@link #projectedColumns} widened by exactly the leaves to read.
     *
     * @param castWarnings   the iterator's sink for values a leaf cannot be converted to the requested type with, or a
     *                       supplier of {@code null} under a strict policy, which fails the read instead
     * @param nullElements   told the collection's path whenever a null element is dropped
     */
    static ParquetChildRelations of(
        MessageType projectedSchema,
        List<ChildRelationRead> reads,
        BlockFactory blockFactory,
        Supplier<SkipWarnings> castWarnings,
        Consumer<String> nullElements
    ) {
        Relation[] relations = new Relation[reads.size()];
        Set<String> paths = new HashSet<>();
        for (int i = 0; i < relations.length; i++) {
            ChildRelationRead read = reads.get(i);
            if (paths.add(read.path()) == false) {
                throw new IllegalArgumentException("child relation [" + read.path() + "] is requested twice");
            }
            relations[i] = Relation.of(projectedSchema, read, blockFactory, castWarnings, nullElements);
        }
        return new ParquetChildRelations(relations);
    }

    boolean isEmpty() {
        return relations.length == 0;
    }

    /**
     * The columns {@code reads} adds to {@code fileSchema}'s projection, as the dotted paths of their descriptors: each
     * requested leaf the file has and, for a collection the file has but none of whose requested leaves it holds, one
     * other leaf whose levels alone say where the elements are.
     */
    static List<String> projectedColumns(MessageType fileSchema, List<ChildRelationRead> reads) {
        List<String> columns = new ArrayList<>();
        for (ChildRelationRead read : reads) {
            ParquetCollections.Shape shape = ParquetCollections.shape(fileSchema, read.path());
            if (shape == null) {
                continue;
            }
            if (shape.collection().unsupported() != null) {
                throw new IllegalArgumentException(shape.collection().unsupported());
            }
            boolean any = false;
            for (ChildRelationRead.Leaf leaf : read.leaves()) {
                if (shape.leaf(leaf.name()) != null) {
                    columns.add(String.join(".", shape.descriptor(fileSchema, leaf.name()).getPath()));
                    any = true;
                }
            }
            if (any == false) {
                columns.add(String.join(".", structuralDescriptor(fileSchema, shape.elementPath()).getPath()));
            }
        }
        return columns;
    }

    /** Every column this read decodes, for the prefetch and the column-read store. */
    List<ColumnDescriptor> descriptors() {
        List<ColumnDescriptor> descriptors = new ArrayList<>();
        for (Relation relation : relations) {
            for (ElementLeaf leaf : relation.decoded()) {
                descriptors.add(leaf.descriptor);
            }
        }
        return descriptors;
    }

    /** Binds the column readers of a new row group, all from {@code store}, the one the row's repeated columns use. */
    void bind(
        ColumnReadStore store,
        ParquetColumnDecoding.ListCorruptionHandler corruptionHandler,
        String fileLocation,
        int rowGroupOrdinal,
        long rowsBeforeGroup
    ) {
        for (Relation relation : relations) {
            for (ElementLeaf leaf : relation.decoded()) {
                leaf.bind(
                    ParquetColumnDecoding.ListColumnReader.bind(
                        store.getColumnReader(leaf.descriptor),
                        leaf.descriptor,
                        corruptionHandler,
                        leaf.columnName,
                        fileLocation,
                        rowGroupOrdinal,
                        rowsBeforeGroup
                    )
                );
            }
        }
    }

    /**
     * Decodes the elements of up to {@code rows} rows and returns how many rows it decoded: all of them, or fewer when
     * the elements reached {@link #BATCH_ELEMENT_BUDGET}. Always at least one row. Releases the windows of the
     * previous parent page first. The decoded elements become the current windows through {@link #finishBatch}, or are
     * dropped through {@link #discardBatch} when the rest of the batch fails.
     */
    int decodeBatch(int rows) {
        releaseCurrent();
        long elements = 0;
        int decoded = 0;
        try {
            while (decoded < rows) {
                for (Relation relation : relations) {
                    elements += relation.readRow(decoded);
                }
                decoded++;
                if (elements >= BATCH_ELEMENT_BUDGET) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            discardBatch();
            throw e;
        }
        return decoded;
    }

    /** Turns the batch {@link #decodeBatch} decoded into the windows of a parent page of {@code parentCount} rows. */
    void finishBatch(int parentCount) {
        ChildWindows[] windows = new ChildWindows[relations.length];
        try {
            for (int i = 0; i < relations.length; i++) {
                windows[i] = relations[i].finishBatch(parentCount);
            }
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(windows);
            discardBatch();
            throw e;
        }
        current = windows;
    }

    void discardBatch() {
        for (Relation relation : relations) {
            relation.discardBatch();
        }
    }

    /** Narrows the current windows to the parents a row drop kept; {@code null} keeps them all. */
    void retainParents(@Nullable int[] survivingParentPositions, int parentCount) {
        if (current == null) {
            return;
        }
        for (ChildWindows windows : current) {
            if (survivingParentPositions != null) {
                windows.retainParents(survivingParentPositions);
            }
            if (windows.parentCount() != parentCount) {
                throw new IllegalStateException(
                    "child windows address [" + windows.parentCount() + "] parents but the page holds [" + parentCount + "]"
                );
            }
        }
    }

    /** See {@code ChildRelationPages#children}. */
    ChildWindows children(String path) {
        for (int i = 0; i < relations.length; i++) {
            if (relations[i].path.equals(path)) {
                if (current == null) {
                    throw new IllegalStateException("no parent page has been read yet");
                }
                return current[i];
            }
        }
        throw new IllegalArgumentException("child relation [" + path + "] was not requested by this read");
    }

    void validateExhausted() {
        for (Relation relation : relations) {
            for (ElementLeaf leaf : relation.decoded()) {
                leaf.input.validateExhausted();
            }
        }
    }

    void releaseCurrent() {
        if (current != null) {
            Releasables.closeExpectNoException(current);
            current = null;
        }
    }

    /** Releases everything, the batch in progress included. */
    void close() {
        releaseCurrent();
        discardBatch();
    }

    /** One column of the element when the file has none of the requested ones, read for its levels. */
    private static ColumnDescriptor structuralDescriptor(MessageType schema, String[] elementPath) {
        for (ColumnDescriptor column : schema.getColumns()) {
            String[] columnPath = column.getPath();
            if (columnPath.length > elementPath.length
                && Arrays.equals(columnPath, 0, elementPath.length, elementPath, 0, elementPath.length)) {
                return column;
            }
        }
        throw new IllegalStateException("collection element [" + String.join(".", elementPath) + "] has no columns");
    }

    /** One collection: its requested leaves, at most one structural leaf, and the leaf that numbers the elements. */
    private static final class Relation {
        private final String path;
        private final List<ChildRelationRead.Leaf> requested;
        /** Per requested leaf; {@code null} for a leaf this file lacks. */
        private final ElementLeaf[] leaves;
        /** The leaf whose levels number the elements and record their parents; {@code null} when the file lacks the collection. */
        @Nullable
        private final ElementLeaf primary;
        /** Decoded leaves: the present requested ones, or the structural one alone. */
        private final List<ElementLeaf> decoded;
        private final Consumer<String> nullElements;

        private Relation(
            String path,
            List<ChildRelationRead.Leaf> requested,
            ElementLeaf[] leaves,
            @Nullable ElementLeaf primary,
            List<ElementLeaf> decoded,
            Consumer<String> nullElements
        ) {
            this.path = path;
            this.requested = requested;
            this.leaves = leaves;
            this.primary = primary;
            this.decoded = decoded;
            this.nullElements = nullElements;
        }

        static Relation of(
            MessageType schema,
            ChildRelationRead read,
            BlockFactory blockFactory,
            Supplier<SkipWarnings> castWarnings,
            Consumer<String> nullElements
        ) {
            String path = read.path();
            ElementLeaf[] leaves = new ElementLeaf[read.leaves().size()];
            ParquetCollections.Shape shape = ParquetCollections.shape(schema, path);
            if (shape == null) {
                // A file without the collection has no elements for it, as a file without a column has nulls for it
                return new Relation(path, read.leaves(), leaves, null, List.of(), nullElements);
            }
            if (shape.collection().unsupported() != null) {
                throw new IllegalArgumentException(shape.collection().unsupported());
            }
            List<ElementLeaf> decoded = new ArrayList<>();
            for (int l = 0; l < leaves.length; l++) {
                ChildRelationRead.Leaf wanted = read.leaves().get(l);
                CollectionSchema.Leaf inFile = shape.leaf(wanted.name());
                if (inFile == null) {
                    continue;
                }
                if (inFile.type() != wanted.type() && DeclaredTypeCoercions.supports(inFile.type(), wanted.type()) == false) {
                    throw new IllegalArgumentException(
                        "["
                            + path
                            + "."
                            + wanted.name()
                            + "] is ["
                            + inFile.type().typeName()
                            + "] in this file but ["
                            + wanted.type().typeName()
                            + "] in the dataset, and an element's field cannot be converted to it"
                    );
                }
                leaves[l] = new ElementLeaf(
                    path + "." + wanted.name(),
                    shape.descriptor(schema, wanted.name()),
                    shape,
                    inFile.type(),
                    wanted.type(),
                    true,
                    blockFactory,
                    castWarnings
                );
                decoded.add(leaves[l]);
            }
            if (decoded.isEmpty()) {
                ColumnDescriptor structural = structuralDescriptor(schema, shape.elementPath());
                decoded.add(new ElementLeaf(path, structural, shape, null, null, false, blockFactory, castWarnings));
            }
            ElementLeaf primary = decoded.getFirst();
            primary.recordParents();
            return new Relation(path, read.leaves(), leaves, primary, List.copyOf(decoded), nullElements);
        }

        List<ElementLeaf> decoded() {
            return decoded;
        }

        /** Decodes row {@code parent} on every leaf and returns its element count, which every leaf must agree on. */
        int readRow(int parent) {
            if (primary == null) {
                return 0;
            }
            int elements = -1;
            for (ElementLeaf leaf : decoded) {
                int count = leaf.readRow(parent, leaf == primary ? () -> nullElements.accept(path) : () -> {});
                if (elements < 0) {
                    elements = count;
                } else if (elements != count) {
                    throw new IllegalStateException("the leaves of collection [" + path + "] disagree on its elements");
                }
            }
            return elements;
        }

        ChildWindows finishBatch(int parentCount) {
            if (primary == null) {
                return ChildWindows.empty(parentCount);
            }
            List<Block> parents = primary.finishParents();
            List<List<Block>> values = new ArrayList<>(leaves.length);
            List<Page> pages = new ArrayList<>(parents.size());
            boolean built = false;
            try {
                for (ElementLeaf leaf : leaves) {
                    values.add(leaf == null ? null : leaf.finishValues());
                }
                for (int p = 0; p < parents.size(); p++) {
                    Block parentBlock = parents.get(p);
                    int count = parentBlock.getPositionCount();
                    Block[] blocks = new Block[leaves.length + 1];
                    blocks[0] = parentBlock;
                    parents.set(p, null);
                    for (int l = 0; l < leaves.length; l++) {
                        List<Block> leafPages = values.get(l);
                        if (leafPages == null) {
                            blocks[l + 1] = parentBlock.blockFactory().newConstantNullBlock(count);
                            continue;
                        }
                        Block leafBlock = leafPages.get(p);
                        leafPages.set(p, null);
                        if (leafBlock.getPositionCount() != count) {
                            Releasables.closeExpectNoException(blocks);
                            Releasables.closeExpectNoException(leafBlock);
                            throw new IllegalStateException("the leaves of collection [" + path + "] disagree on its elements");
                        }
                        blocks[l + 1] = leafBlock;
                    }
                    pages.add(new Page(count, blocks));
                }
                built = true;
                return new ChildWindows(parentCount, pages);
            } finally {
                if (built == false) {
                    Releasables.closeExpectNoException(parents.toArray(Block[]::new));
                    for (List<Block> leafPages : values) {
                        if (leafPages != null) {
                            Releasables.closeExpectNoException(leafPages.toArray(Block[]::new));
                        }
                    }
                    for (Page page : pages) {
                        page.releaseBlocks();
                    }
                }
            }
        }

        void discardBatch() {
            for (ElementLeaf leaf : decoded) {
                leaf.discard();
            }
        }

        @Override
        public String toString() {
            return path + requested;
        }
    }

    /** How one leaf's physical values become the values of its type, as the carrier decode converts them. */
    private enum ValueKind {
        BOOLEAN,
        INT,
        DAYS_AS_MILLIS,
        UNSIGNED_INT_AS_LONG,
        LONG,
        MICROS_AS_NANOS,
        FLOAT_AS_DOUBLE,
        DOUBLE,
        BYTES;

        static ValueKind of(PrimitiveType primitive, DataType fileType) {
            LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
            return switch (primitive.getPrimitiveTypeName()) {
                case BOOLEAN -> BOOLEAN;
                case INT32 -> switch (fileType) {
                    case DATETIME -> DAYS_AS_MILLIS;
                    case LONG -> UNSIGNED_INT_AS_LONG;
                    case INTEGER -> INT;
                    default -> throw new IllegalStateException(
                        "unexpected collection leaf type [" + fileType + "] for [" + primitive + "]"
                    );
                };
                case INT64 -> logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts
                    && ts.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS ? MICROS_AS_NANOS : LONG;
                case FLOAT -> FLOAT_AS_DOUBLE;
                case DOUBLE -> DOUBLE;
                case BINARY -> BYTES;
                case INT96, FIXED_LEN_BYTE_ARRAY -> throw new IllegalStateException("unsupported collection leaf [" + primitive + "]");
            };
        }
    }

    /**
     * One decoded column of an element, appending each element's values to a block builder at the file's type and
     * cutting a page every {@link ChildWindows#MAX_ELEMENTS_PER_PAGE} elements. A structural leaf appends no values.
     */
    private static final class ElementLeaf {
        private final String columnName;
        private final ColumnDescriptor descriptor;
        /** The repetition level of a new element of the collection. */
        private final int collectionRep;
        /** The definition level at which the collection has an element at all. */
        private final int existsDef;
        /** The definition level at which that element is not null. */
        private final int elementDef;
        private final int maxDef;
        private final boolean emitsValues;
        @Nullable
        private final DataType fileType;
        @Nullable
        private final DataType emitType;
        @Nullable
        private final ValueKind kind;
        private final BlockFactory blockFactory;
        private final Supplier<SkipWarnings> castWarnings;
        private final BytesRef scratch = new BytesRef();

        private boolean recordsParents;
        private ParquetColumnDecoding.ListColumnReader input;
        private Block.Builder builder;
        private IntVector.Builder parentBuilder;
        private final List<Block> pages = new ArrayList<>();
        private final List<Block> parentPages = new ArrayList<>();
        private int inPage;
        private int valuesInElement;

        ElementLeaf(
            String columnName,
            ColumnDescriptor descriptor,
            ParquetCollections.Shape shape,
            @Nullable DataType fileType,
            @Nullable DataType emitType,
            boolean emitsValues,
            BlockFactory blockFactory,
            Supplier<SkipWarnings> castWarnings
        ) {
            this.columnName = columnName;
            this.descriptor = descriptor;
            this.collectionRep = shape.collectionRep();
            this.existsDef = shape.existsDef();
            this.elementDef = shape.elementDef();
            this.maxDef = descriptor.getMaxDefinitionLevel();
            this.emitsValues = emitsValues;
            this.fileType = fileType;
            this.emitType = emitType;
            this.kind = emitsValues ? ValueKind.of(descriptor.getPrimitiveType(), fileType) : null;
            this.blockFactory = blockFactory;
            this.castWarnings = castWarnings;
        }

        void recordParents() {
            this.recordsParents = true;
        }

        void bind(ParquetColumnDecoding.ListColumnReader input) {
            this.input = input;
        }

        /**
         * Reads one row's levels, opening an element at each repetition level that starts one. Returns the row's element
         * count. The walk is the carrier decode's ({@code ParquetCollections.Leaf#read}), on a cursor that validates the
         * levels and charges a malformed list to the read's error budget.
         */
        int readRow(int parent, Runnable nullElement) {
            input.ensureRowStart();
            int elements = 0;
            boolean open = false;
            boolean first = true;
            while (first || input.hasRemaining() && input.currentRepetitionLevel() > 0) {
                first = false;
                int rep = input.currentRepetitionLevel();
                int def = input.currentDefinitionLevel();
                if (def < existsDef) {
                    // A null or empty collection: no elements
                    input.skipCurrentValue();
                    continue;
                }
                if (rep <= collectionRep) {
                    if (open) {
                        closeElement();
                    }
                    if (def < elementDef) {
                        nullElement.run();
                        open = false;
                        input.skipCurrentValue();
                        continue;
                    }
                    openElement(parent);
                    open = true;
                    elements++;
                }
                if (open && emitsValues && def == maxDef) {
                    appendValue(input.reader());
                    input.consumeAfterRead();
                } else {
                    input.skipCurrentValue();
                }
            }
            if (open) {
                closeElement();
            }
            input.rowCompleted();
            return elements;
        }

        private void openElement(int parent) {
            if (inPage == ChildWindows.MAX_ELEMENTS_PER_PAGE) {
                flushPage();
            }
            if (recordsParents) {
                if (parentBuilder == null) {
                    parentBuilder = blockFactory.newIntVectorBuilder(ChildWindows.MAX_ELEMENTS_PER_PAGE);
                }
                parentBuilder.appendInt(parent);
            }
            if (emitsValues && builder == null) {
                builder = newBuilder();
            }
            inPage++;
            valuesInElement = 0;
        }

        /** An element with no value is a null rather than an empty position, as in a list column. */
        private void closeElement() {
            if (emitsValues == false) {
                return;
            }
            if (valuesInElement > 0) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }

        private void appendValue(ColumnReader reader) {
            if (valuesInElement == 0) {
                builder.beginPositionEntry();
            }
            valuesInElement++;
            switch (kind) {
                case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(reader.getBoolean());
                case INT -> ((IntBlock.Builder) builder).appendInt(reader.getInteger());
                case DAYS_AS_MILLIS -> ((LongBlock.Builder) builder).appendLong(TimeUnit.DAYS.toMillis(reader.getInteger()));
                case UNSIGNED_INT_AS_LONG -> ((LongBlock.Builder) builder).appendLong(Integer.toUnsignedLong(reader.getInteger()));
                case LONG -> ((LongBlock.Builder) builder).appendLong(reader.getLong());
                case MICROS_AS_NANOS -> ((LongBlock.Builder) builder).appendLong(reader.getLong() * 1000L);
                case FLOAT_AS_DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(reader.getFloat());
                case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(reader.getDouble());
                case BYTES -> ((BytesRefBlock.Builder) builder).appendBytesRef(bytes(reader.getBinary()));
            }
        }

        private BytesRef bytes(Binary binary) {
            ByteBuffer buffer = binary.toByteBuffer();
            if (buffer.hasArray()) {
                scratch.bytes = buffer.array();
                scratch.offset = buffer.arrayOffset() + buffer.position();
                scratch.length = buffer.remaining();
                return scratch;
            }
            return new BytesRef(binary.getBytes());
        }

        private Block.Builder newBuilder() {
            int size = ChildWindows.MAX_ELEMENTS_PER_PAGE;
            return switch (fileType) {
                case BOOLEAN -> blockFactory.newBooleanBlockBuilder(size);
                case INTEGER -> blockFactory.newIntBlockBuilder(size);
                case LONG, DATETIME, DATE_NANOS -> blockFactory.newLongBlockBuilder(size);
                case DOUBLE -> blockFactory.newDoubleBlockBuilder(size);
                case KEYWORD -> blockFactory.newBytesRefBlockBuilder(size);
                default -> throw new IllegalStateException("unsupported collection leaf type [" + fileType + "]");
            };
        }

        private void flushPage() {
            if (inPage == 0) {
                return;
            }
            if (recordsParents) {
                IntVector.Builder parents = parentBuilder;
                parentBuilder = null;
                try (parents) {
                    parentPages.add(parents.build().asBlock());
                }
            }
            if (emitsValues) {
                Block.Builder values = builder;
                builder = null;
                Block physical;
                try (values) {
                    physical = values.build();
                }
                if (fileType == emitType) {
                    pages.add(physical);
                } else {
                    try (physical) {
                        pages.add(
                            DeclaredTypeCoercions.castBlock(
                                physical,
                                fileType,
                                emitType,
                                null,
                                blockFactory,
                                columnName,
                                castWarnings.get()
                            )
                        );
                    }
                }
            }
            inPage = 0;
        }

        /** The parent-position blocks of the batch, one per page; the caller owns them. */
        List<Block> finishParents() {
            flushPage();
            List<Block> out = new ArrayList<>(parentPages);
            parentPages.clear();
            return out;
        }

        /** The value blocks of the batch, one per page; the caller owns them. */
        List<Block> finishValues() {
            flushPage();
            List<Block> out = new ArrayList<>(pages);
            pages.clear();
            return out;
        }

        void discard() {
            Releasables.closeExpectNoException(builder, parentBuilder);
            builder = null;
            parentBuilder = null;
            Releasables.closeExpectNoException(pages.toArray(Block[]::new));
            Releasables.closeExpectNoException(parentPages.toArray(Block[]::new));
            pages.clear();
            parentPages.clear();
            inPage = 0;
        }
    }
}
