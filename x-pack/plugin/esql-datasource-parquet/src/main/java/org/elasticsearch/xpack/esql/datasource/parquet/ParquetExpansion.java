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
import org.apache.parquet.io.ParquetDecodingException;
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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.CollectionSchema;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnarRowDropHelper;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandedRows;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * The Parquet half of an {@link ExpandRead}: the requested element leaves of one collection, decoded beside the rows
 * through the same column-read store as the row's own repeated columns, and joined to the batch's parent rows with
 * {@link ExpandedRows} so the iterator emits expanded rows. That store holds one stateful page reader per column, so the
 * collection is decoded once per scan, beside the rows rather than in a second pass that would need a reader of its own.
 * <p>
 * The decode runs ahead of the row's columns for each batch and may end the batch early: once the rows the batch expands
 * to (each parent's elements, or one null row for a parent without) reach the read's batch size, it stops at that row,
 * and the iterator reads the other columns for that many rows only. So an expanded page holds at most a batch of rows
 * plus one parent's elements; a single parent larger than the batch is held whole, charged to the breaker like every
 * other block. The filters, drops and row limit of the batch then apply to its parents, and only the survivors expand.
 * <p>
 * What an element is comes from {@link ParquetCollections.Shape}, the levels the carrier decode uses too, and the
 * element semantics match it: a null element is dropped and announced, an empty or null list has no elements, a leaf the
 * file lacks is null on every element, and a list inside the element is that leaf's multivalue.
 */
final class ParquetExpansion {
    private static final Logger logger = LogManager.getLogger(ParquetExpansion.class);

    /**
     * What one file's read of an {@link ExpandRead} needs before an iterator exists: the collection's shape, taken from
     * the FILE schema, and the leaves read as null because this file cannot supply them at the requested type.
     * <p>
     * The shape must come from the file schema, never the projected one: the projection keeps only the requested leaves
     * of the element, and a legacy two-level list whose element is a group of several fields, pruned to one, reads as a
     * list of that one primitive. Levels do not depend on sibling fields, so the shape stays valid for the projection.
     *
     * @param shape         the collection's shape in the file, or {@code null} when the file has no such collection
     * @param nulled        requested leaves this file stores at a type that does not widen to the requested one, read as
     *                      null under every error policy
     * @param absent        the dotted names of what the read asks for and this file lacks, the collection itself or some
     *                      of its leaves: they read as null and are announced as an absent column is
     * @param parentColumns the leading blocks of the iterator's page that are the projection; any after them are the
     *                      pushed filter's own columns and are not part of the output
     * @param rowBudget     the rows one batch may expand to, the read's batch size
     */
    record Spec(
        ExpandRead read,
        @Nullable ParquetCollections.Shape shape,
        Set<String> nulled,
        List<String> absent,
        int parentColumns,
        int rowBudget,
        String fileLocation,
        ErrorPolicy errorPolicy,
        @Nullable Consumer<String> warningSink
    ) {

        /**
         * Resolves {@code read} against {@code fileSchema}. A leaf's requested type is the one schema discovery merged
         * across the files ({@link TypeWidening#join}): an inferred type, not a declared one. So a leaf is handled as an
         * inferred column is ({@code ParquetFormatReader#validatePlannerTypesAgainstFile}), together with the
         * reconciliation cast that merge implies ({@code ColumnMapping#alignToQuery}): a file storing a type that widens
         * to the requested one converts each value, and a file storing any other type reads the leaf as null, with a
         * warning, under every error policy. A value the widening cannot represent is the error policy's; see
         * {@link #expand}.
         */
        static Spec of(
            MessageType fileSchema,
            ExpandRead read,
            int parentColumns,
            int rowBudget,
            String fileLocation,
            ErrorPolicy errorPolicy,
            @Nullable Consumer<String> warningSink
        ) {
            ParquetCollections.Shape shape = ParquetCollections.shape(fileSchema, read.path());
            Set<String> nulled = new HashSet<>();
            List<String> absent = new ArrayList<>();
            if (shape == null) {
                absent.add(read.path());
            } else {
                if (shape.collection().unsupported() != null) {
                    throw new IllegalArgumentException(shape.collection().unsupported());
                }
                List<String> nullDetails = new ArrayList<>();
                for (ExpandRead.Leaf wanted : read.leaves()) {
                    String name = read.path() + "." + wanted.name();
                    CollectionSchema.Leaf inFile = shape.leaf(wanted.name());
                    if (inFile == null) {
                        absent.add(name);
                        continue;
                    }
                    if (widens(inFile.type(), wanted.type())) {
                        continue;
                    }
                    nullDetails.add(DeclaredTypeCoercions.uncoercibleColumnDetail(name, inFile.type(), wanted.type()));
                    logger.warn(
                        "Column [{}] in [{}] is [{}] in the file, [{}] in the query; returning null",
                        name,
                        fileLocation,
                        inFile.type().typeName(),
                        wanted.type().typeName()
                    );
                    nulled.add(wanted.name());
                }
                if (nullDetails.isEmpty() == false) {
                    SkipWarnings nullWarnings = new SkipWarnings(
                        DeclaredTypeCoercions.uncoercibleColumnsNullSummary(fileLocation),
                        warningSink
                    );
                    nullDetails.forEach(nullWarnings::add);
                }
            }
            return new Spec(
                read,
                shape,
                Set.copyOf(nulled),
                List.copyOf(absent),
                parentColumns,
                rowBudget,
                fileLocation,
                errorPolicy,
                warningSink
            );
        }

        /**
         * Whether a leaf the file stores as {@code inFile} reads as {@code wanted}: the same type, or a widening the merge
         * across files produces, which a reconciliation cast converts value by value. A narrowing or an unrelated type
         * is not what discovery merged for a file storing it, so the leaf reads as null, as an inferred column does.
         */
        static boolean widens(DataType inFile, DataType wanted) {
            return inFile == wanted || (TypeWidening.join(inFile, wanted) == wanted && DeclaredTypeCoercions.supports(inFile, wanted));
        }

        /**
         * The columns the read adds to the file's projection, as the dotted paths of their descriptors: each requested
         * leaf the file has and can supply, or, when there is none, one other leaf whose levels alone say where the
         * elements are.
         */
        List<String> projectedColumns(MessageType fileSchema) {
            if (shape == null) {
                return List.of();
            }
            List<String> columns = new ArrayList<>();
            for (ExpandRead.Leaf leaf : read.leaves()) {
                if (decodes(leaf)) {
                    columns.add(String.join(".", shape.descriptor(fileSchema, leaf.name()).getPath()));
                }
            }
            if (columns.isEmpty()) {
                columns.add(String.join(".", structuralDescriptor(fileSchema, shape.elementPath()).getPath()));
            }
            return columns;
        }

        private boolean decodes(ExpandRead.Leaf leaf) {
            return shape.leaf(leaf.name()) != null && nulled.contains(leaf.name()) == false;
        }
    }

    private final Spec spec;
    private final BlockFactory blockFactory;
    /** Per requested leaf; {@code null} for a leaf this file lacks or cannot supply. */
    private final ElementLeaf[] leaves;
    /** The leaf whose levels number the elements and record their parents; {@code null} when the file lacks the collection. */
    @Nullable
    private final ElementLeaf primary;
    /** Decoded leaves: the present requested ones, or the structural one alone. */
    private final List<ElementLeaf> decoded;
    private final Consumer<String> nullElements;
    /** Created on the first value a leaf cannot be converted with; see {@link #castWarnings()}. */
    @Nullable
    private SkipWarnings castWarnings;
    /** The rows of the batch {@link #decodeBatch} decoded and {@link #expand} has not yet joined; {@code -1} for none. */
    private int batchRows = -1;
    /** The parents the last {@link #expand} dropped for an element value it could not convert, under {@code skip_row}. */
    private int lastDroppedParents;

    private ParquetExpansion(
        Spec spec,
        BlockFactory blockFactory,
        ElementLeaf[] leaves,
        @Nullable ElementLeaf primary,
        List<ElementLeaf> decoded,
        Consumer<String> nullElements
    ) {
        this.spec = spec;
        this.blockFactory = blockFactory;
        this.leaves = leaves;
        this.primary = primary;
        this.decoded = decoded;
        this.nullElements = nullElements;
    }

    /**
     * The decode of {@code spec}, its descriptors resolved against {@code projectedSchema}, the schema the iterator
     * reads, which {@link Spec#projectedColumns} widened by exactly the leaves to decode.
     *
     * @param nullElements told the collection's path whenever a null element is dropped
     */
    static ParquetExpansion of(Spec spec, MessageType projectedSchema, BlockFactory blockFactory, Consumer<String> nullElements) {
        ExpandRead read = spec.read();
        ParquetCollections.Shape shape = spec.shape();
        ElementLeaf[] leaves = new ElementLeaf[read.leaves().size()];
        if (shape == null) {
            // A file without the collection has no elements for it, as a file without a column has nulls for it
            return new ParquetExpansion(spec, blockFactory, leaves, null, List.of(), nullElements);
        }
        List<ElementLeaf> decoded = new ArrayList<>();
        for (int l = 0; l < leaves.length; l++) {
            ExpandRead.Leaf wanted = read.leaves().get(l);
            if (spec.decodes(wanted) == false) {
                continue;
            }
            leaves[l] = new ElementLeaf(
                read.path() + "." + wanted.name(),
                shape.descriptor(projectedSchema, wanted.name()),
                shape,
                shape.leaf(wanted.name()).type(),
                wanted.type(),
                true,
                blockFactory
            );
            decoded.add(leaves[l]);
        }
        if (decoded.isEmpty()) {
            ColumnDescriptor structural = structuralDescriptor(projectedSchema, shape.elementPath());
            decoded.add(new ElementLeaf(read.path(), structural, shape, null, null, false, blockFactory));
        }
        ElementLeaf primary = decoded.getFirst();
        primary.recordParents();
        return new ParquetExpansion(spec, blockFactory, leaves, primary, List.copyOf(decoded), nullElements);
    }

    String path() {
        return spec.read().path();
    }

    /** Every column this read decodes, for the prefetch and the column-read store. */
    List<ColumnDescriptor> descriptors() {
        List<ColumnDescriptor> descriptors = new ArrayList<>();
        for (ElementLeaf leaf : decoded) {
            descriptors.add(leaf.descriptor);
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
        for (ElementLeaf leaf : decoded) {
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

    /**
     * Decodes the elements of up to {@code rows} rows and returns how many rows it decoded: all of them, or fewer when
     * the rows they expand to reached the batch budget. Always at least one row. The batch is then joined to its parent
     * rows by {@link #expand}, or dropped by {@link #discardBatch} when the rest of the batch fails.
     */
    int decodeBatch(int rows) {
        if (batchRows >= 0) {
            throw new IllegalStateException("the elements of the previous batch of [" + path() + "] were never expanded");
        }
        long expanded = 0;
        int decoded = 0;
        try {
            while (decoded < rows) {
                expanded += Math.max(1, readRow(decoded));
                decoded++;
                if (expanded >= spec.rowBudget()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            discardBatch();
            throw e;
        }
        batchRows = decoded;
        return decoded;
    }

    /** Decodes row {@code parent} on every leaf and returns its element count, which every leaf must agree on. */
    private int readRow(int parent) {
        if (primary == null) {
            return 0;
        }
        int elements = -1;
        for (ElementLeaf leaf : decoded) {
            int count = leaf.readRow(parent, leaf == primary ? () -> nullElements.accept(path()) : () -> {});
            if (elements < 0) {
                elements = count;
            } else if (elements != count) {
                throw disagreement();
            }
        }
        return elements;
    }

    /**
     * Joins the batch {@link #decodeBatch} decoded to {@code parents}, the rows of that batch the iterator kept, and
     * returns the expanded page. Takes ownership of {@code parents}, releasing it whether or not this succeeds.
     * <p>
     * Under {@code skip_row}, an element value that cannot be converted to its leaf's type drops the element's parent,
     * every row it would expand to, as a column value that cannot be converted drops its row. The iterator's row limit
     * counted the batch's parents before this drop, so it gives them back by {@link #lastDroppedParents()}, and charges
     * them, one error per parent, with {@link #chargeDroppedParents} outside its failure wrapping: a spent budget is a
     * fault of the client's data, not a failure to read the file.
     *
     * @param survivors the batch positions {@code parents} holds, ascending, or {@code null} when it holds them all
     * @param skipRow   whether the read's policy is {@code skip_row}
     */
    Page expand(Page parents, @Nullable int[] survivors, boolean skipRow) {
        int rows = batchRows;
        batchRows = -1;
        lastDroppedParents = 0;
        IntVector elementParents = null;
        Block[] values = new Block[leaves.length];
        Page projected = null;
        try {
            if (rows < 0) {
                throw new IllegalStateException("no elements of [" + path() + "] were decoded for this batch");
            }
            // Owned by the projection from here, so the failure path below cannot release it twice
            Page input = parents;
            parents = null;
            projected = projection(input);
            if (primary == null) {
                elementParents = blockFactory.newConstantIntVector(0, 0);
            } else {
                elementParents = primary.finishParents();
            }
            FailedElements failed = skipRow ? new FailedElements(elementParents.getPositionCount()) : null;
            for (int l = 0; l < leaves.length; l++) {
                if (leaves[l] != null) {
                    values[l] = leaves[l].finishValues(this::castWarnings, failed);
                }
            }
            if (failed != null && failed.any()) {
                int[] kept = keptParents(failed, elementParents, survivors, rows, projected.getPositionCount());
                int dropped = projected.getPositionCount() - kept.length;
                Page narrowed = projected.filter(false, keptPositionsInPage(failed, elementParents, survivors, rows));
                projected.releaseBlocks();
                projected = narrowed;
                survivors = kept;
                lastDroppedParents = dropped;
            }
        } catch (RuntimeException e) {
            discardBatch();
            Releasables.closeExpectNoException(elementParents);
            Releasables.closeExpectNoException(values);
            if (projected != null) {
                projected.releaseBlocks();
            }
            if (parents != null) {
                parents.releaseBlocks();
            }
            throw e;
        }
        return ExpandedRows.expand(projected, survivors, rows, elementParents, values, spec.read().parentOrdinals(), blockFactory);
    }

    /**
     * Charges the parents the last {@link #expand} dropped to {@code dropHelper}'s budget, one error each, and fails the
     * read when that spends it. Releases {@code page}, the expanded page, when it fails.
     */
    void chargeDroppedParents(@Nullable ColumnarRowDropHelper dropHelper, Page page) {
        if (lastDroppedParents == 0 || dropHelper == null) {
            return;
        }
        try {
            dropHelper.addToTotals(0, lastDroppedParents);
            dropHelper.checkBudget();
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
    }

    /** The parents the last {@link #expand} dropped under {@code skip_row} for an element value it could not convert. */
    int lastDroppedParents() {
        return lastDroppedParents;
    }

    /** The batch positions whose parent survives the batch's earlier drops and has no element that failed to convert. */
    private static int[] keptParents(FailedElements failed, IntVector elementParents, @Nullable int[] survivors, int rows, int parents) {
        boolean[] failedParent = failedParents(failed, elementParents, rows);
        int[] kept = new int[parents];
        int k = 0;
        for (int p = 0; p < parents; p++) {
            int batchPosition = survivors == null ? p : survivors[p];
            if (failedParent[batchPosition] == false) {
                kept[k++] = batchPosition;
            }
        }
        return Arrays.copyOf(kept, k);
    }

    /** The positions, in the page of surviving parents, of the parents {@link #keptParents} keeps. */
    private static int[] keptPositionsInPage(FailedElements failed, IntVector elementParents, @Nullable int[] survivors, int rows) {
        boolean[] failedParent = failedParents(failed, elementParents, rows);
        int parents = survivors == null ? rows : survivors.length;
        int[] kept = new int[parents];
        int k = 0;
        for (int p = 0; p < parents; p++) {
            int batchPosition = survivors == null ? p : survivors[p];
            if (failedParent[batchPosition] == false) {
                kept[k++] = p;
            }
        }
        return Arrays.copyOf(kept, k);
    }

    private static boolean[] failedParents(FailedElements failed, IntVector elementParents, int rows) {
        boolean[] failedParent = new boolean[rows];
        for (int e = 0; e < elementParents.getPositionCount(); e++) {
            if (failed.at(e)) {
                failedParent[elementParents.getInt(e)] = true;
            }
        }
        return failedParent;
    }

    /** The elements of a batch whose value of some leaf could not be converted, under {@code skip_row}. */
    private static final class FailedElements implements IntConsumer {
        private final boolean[] failed;
        private boolean any;

        FailedElements(int elements) {
            this.failed = new boolean[elements];
        }

        @Override
        public void accept(int element) {
            failed[element] = true;
            any = true;
        }

        boolean any() {
            return any;
        }

        boolean at(int element) {
            return failed[element];
        }
    }

    /**
     * {@code page} without the trailing blocks of the pushed filter's own columns, which are not part of the output.
     * Takes ownership of {@code page}: the trailing blocks are released only once the page they leave is built, and
     * that release cannot throw, so no block is released twice.
     */
    private Page projection(Page page) {
        int keep = spec.parentColumns();
        if (page.getBlockCount() == keep) {
            return page;
        }
        Block[] kept = new Block[keep];
        Block[] extra = new Block[page.getBlockCount() - keep];
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (b < keep) {
                kept[b] = page.getBlock(b);
            } else {
                extra[b - keep] = page.getBlock(b);
            }
        }
        Page projected = new Page(page.getPositionCount(), kept);
        Releasables.closeExpectNoException(extra);
        return projected;
    }

    /**
     * The sink for values of a leaf that cannot be converted to the requested type, or {@code null} under a strict
     * policy, which fails the read instead. Its own sink rather than the iterator's, which names the file's columns:
     * this one names the collection. Under {@code skip_row} the parent of such a value is dropped ({@link #expand});
     * under any other lenient policy the value is null and its row kept.
     */
    @Nullable
    private SkipWarnings castWarnings() {
        if (spec.errorPolicy().isStrict()) {
            return null;
        }
        if (castWarnings == null) {
            String prefix = "Some values of [" + path() + "] in [" + spec.fileLocation() + "] cannot be read as their merged type; ";
            castWarnings = new SkipWarnings(
                spec.errorPolicy().mode() == ErrorPolicy.Mode.SKIP_ROW ? prefix + "skipping their rows" : prefix + "returning null",
                prefix + SkipWarnings.REMOVED_FROM_MULTI_VALUE_OUTCOME,
                spec.warningSink()
            );
        }
        return castWarnings;
    }

    /** Drops the batch in progress, if any. */
    void discardBatch() {
        batchRows = -1;
        for (ElementLeaf leaf : decoded) {
            leaf.discard();
        }
    }

    void validateExhausted() {
        for (ElementLeaf leaf : decoded) {
            leaf.input.validateExhausted();
        }
    }

    /** Releases everything, the batch in progress included. Safe to call more than once. */
    void close() {
        discardBatch();
    }

    /**
     * Leaves of one collection that disagree on how many elements a row has, which only a corrupt file has. A decoding
     * failure, so the iterator's failure wrapping names the file, row group and batch.
     */
    private ParquetDecodingException disagreement() {
        return new ParquetDecodingException("the leaves of collection [" + path() + "] disagree on its elements");
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
     * One decoded column of an element, appending each element's values to one block builder per batch at the file's
     * type. A structural leaf appends no values. The primary leaf also records each element's parent row.
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
        private final BytesRef scratch = new BytesRef();

        private boolean recordsParents;
        private ParquetColumnDecoding.ListColumnReader input;
        @Nullable
        private Block.Builder builder;
        @Nullable
        private IntVector.Builder parentBuilder;
        private int valuesInElement;

        ElementLeaf(
            String columnName,
            ColumnDescriptor descriptor,
            ParquetCollections.Shape shape,
            @Nullable DataType fileType,
            @Nullable DataType emitType,
            boolean emitsValues,
            BlockFactory blockFactory
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
            if (recordsParents) {
                if (parentBuilder == null) {
                    parentBuilder = blockFactory.newIntVectorBuilder(16);
                }
                parentBuilder.appendInt(parent);
            }
            if (emitsValues && builder == null) {
                builder = newBuilder();
            }
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
            int size = 16;
            return switch (fileType) {
                case BOOLEAN -> blockFactory.newBooleanBlockBuilder(size);
                case INTEGER -> blockFactory.newIntBlockBuilder(size);
                case LONG, DATETIME, DATE_NANOS -> blockFactory.newLongBlockBuilder(size);
                case DOUBLE -> blockFactory.newDoubleBlockBuilder(size);
                case KEYWORD -> blockFactory.newBytesRefBlockBuilder(size);
                default -> throw new IllegalStateException("unsupported collection leaf type [" + fileType + "]");
            };
        }

        /** The parent row of every element of the batch, in element order; the caller owns it. */
        IntVector finishParents() {
            IntVector.Builder parents = parentBuilder;
            parentBuilder = null;
            if (parents == null) {
                return blockFactory.newConstantIntVector(0, 0);
            }
            try (parents) {
                return parents.build();
            }
        }

        /**
         * The values of every element of the batch at the requested type; the caller owns the block. A value the cast
         * cannot represent is null, and its element is told to {@code failed} when one is given.
         */
        Block finishValues(Supplier<SkipWarnings> castWarnings, @Nullable IntConsumer failed) {
            Block.Builder values = builder;
            builder = null;
            if (values == null) {
                return blockFactory.newConstantNullBlock(0);
            }
            Block physical;
            try (values) {
                physical = values.build();
            }
            if (fileType == emitType) {
                return physical;
            }
            try (physical) {
                return DeclaredTypeCoercions.castBlock(
                    physical,
                    fileType,
                    emitType,
                    null,
                    blockFactory,
                    columnName,
                    castWarnings.get(),
                    failed
                );
            }
        }

        void discard() {
            Releasables.closeExpectNoException(builder, parentBuilder);
            builder = null;
            parentBuilder = null;
        }
    }
}
