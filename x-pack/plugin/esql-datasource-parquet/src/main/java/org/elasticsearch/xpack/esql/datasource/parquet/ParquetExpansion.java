/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandedRows;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * What an element is comes from {@link ParquetCollections.Shape}, the levels at which a list has an element and the
 * element is not null: a null element is dropped and announced, an empty or null list has no elements, a leaf the file
 * lacks is null on every element, and a list inside the element is that leaf's multivalue.
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
     * @param expandedBatchRows the rows one batch may expand to, the read's batch size
     */
    record Spec(
        ExpandRead read,
        @Nullable ParquetCollections.Shape shape,
        Set<String> nulled,
        List<String> absent,
        int parentColumns,
        int expandedBatchRows,
        String fileLocation,
        ErrorPolicy errorPolicy,
        @Nullable Consumer<String> warningSink
    ) {

        /**
         * Resolves {@code read} against {@code fileSchema}. A leaf's requested type is the one schema discovery merged
         * across the files ({@link TypeWidening#join}): an inferred type, not a declared one. So a leaf is handled as an
         * inferred column is: a file storing a type that widens to the requested one converts each value, as the
         * reconciliation cast the merge implies does, and a file storing any other type reads the leaf as null, with a
         * warning, under every error policy. A value the widening cannot represent is the error policy's; see
         * {@link #expand}.
         * <p>
         * For a type that does not widen, the precedent followed is the reader's for an inferred column
         * ({@code ParquetFormatReader#validatePlannerTypesAgainstFile}, null with a warning), not the reconciliation
         * adapter's ({@code ColumnMapping#alignToQuery}, which fails the read at open). The adapter's failure guards a
         * pair the planner should never have produced for a column it maps; a leaf is chosen by the query from a merged
         * description, and failing there would fail every query that expands the collection over a file that cannot
         * supply one of the leaves, while the other leaves and the rows of that file read fine.
         */
        static Spec of(
            MessageType fileSchema,
            int maxLeaves,
            ExpandRead read,
            int parentColumns,
            int expandedBatchRows,
            String fileLocation,
            ErrorPolicy errorPolicy,
            @Nullable Consumer<String> warningSink
        ) {
            ParquetCollections.Shape shape = ParquetCollections.shape(fileSchema, read.path(), maxLeaves);
            Set<String> nulled = new HashSet<>();
            List<String> absent = new ArrayList<>();
            if (shape == null) {
                absent.add(read.path());
            } else {
                // An element wider than the reader's field cap, which discovery described as unreadable: the analyzer
                // refuses its expansion at the command, so only a file wider than the ones discovery read reaches here
                if (shape.collection().unsupported() != null) {
                    throw new IllegalArgumentException(shape.collection().unsupported());
                }
                List<String> nullDetails = new ArrayList<>();
                for (ExpandRead.Leaf wanted : read.leaves()) {
                    String name = read.path() + "." + wanted.name();
                    ParquetCollections.Leaf inFile = shape.leaf(wanted.name());
                    if (inFile == null) {
                        absent.add(name);
                        continue;
                    }
                    // A leaf no file can supply reads as nulls, as an unsupported column does; one this file cannot supply
                    // but another can is nulled with a warning below, as a column of a type that does not widen is
                    if (wanted.type() == DataType.UNSUPPORTED
                        || (inFile.type() != DataType.UNSUPPORTED && widens(inFile.type(), wanted.type()))) {
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
                expandedBatchRows,
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
            ParquetCollections.Leaf inFile = shape.leaf(leaf.name());
            return inFile != null && nulled.contains(leaf.name()) == false && unsupported(inFile, leaf) == false;
        }

        /** Whether the leaf is one the reader cannot read, in this file or as merged: it reads as nulls without decoding. */
        private static boolean unsupported(ParquetCollections.Leaf inFile, ExpandRead.Leaf wanted) {
            return inFile.type() == DataType.UNSUPPORTED || wanted.type() == DataType.UNSUPPORTED;
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
                if (expanded >= spec.expandedBatchRows()) {
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
            int count = leaf.readRow(parent);
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
     * counted the batch's parents before this drop, so {@link #expandBatch} returns them for the iterator to give back
     * and to charge, one error per parent, with its own row drops to the read's one error budget.
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
        FailedElements failed = null;
        try {
            if (rows < 0) {
                throw new IllegalStateException("no elements of [" + path() + "] were decoded for this batch");
            }
            projected = projection(parents);
            // Owned by the projection from here, so the failure path below cannot release it twice
            parents = null;
            if (primary == null) {
                elementParents = blockFactory.newConstantIntVector(0, 0);
            } else {
                elementParents = primary.finishParents();
            }
            if (skipRow) {
                failed = new FailedElements(elementParents.getPositionCount(), blockFactory.breaker());
            }
            for (int l = 0; l < leaves.length; l++) {
                if (leaves[l] != null) {
                    values[l] = leaves[l].finishValues(this::castWarnings, failed, spec.warningSink());
                }
            }
            if (failed != null && failed.any()) {
                KeptParents kept = failed.keptParents(elementParents, survivors, rows);
                Page narrowed = projected.filter(false, kept.pagePositions());
                projected.releaseBlocks();
                projected = narrowed;
                lastDroppedParents = kept.dropped();
                survivors = kept.batchPositions();
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
        } finally {
            Releasables.closeExpectNoException(failed);
        }
        Page expanded = ExpandedRows.expand(projected, survivors, rows, elementParents, values, blockFactory);
        announceNullElements();
        return expanded;
    }

    /**
     * Announces, once per batch and only for a batch that was expanded, the null elements the decode dropped: of the
     * collection, and of a list inside the element, as a list column announces its own.
     */
    private void announceNullElements() {
        if (primary != null && primary.droppedNullElement) {
            nullElements.accept(path());
        }
        for (ElementLeaf leaf : decoded) {
            if (leaf.droppedNullInnerValue) {
                nullElements.accept(leaf.columnName);
            }
            leaf.droppedNullElement = false;
            leaf.droppedNullInnerValue = false;
        }
    }

    /**
     * One batch's expansion.
     *
     * @param page           the expanded page, which the caller owns
     * @param droppedParents the parents dropped under {@code skip_row} for an element value that could not be converted;
     *                       the caller charges them to the read's one error budget with its own row drops
     */
    record Batch(Page page, int droppedParents) {}

    /**
     * The expansion step both iterators run after reading a batch's parents: {@link #expand}, with a failure reported as a
     * failure to read this batch. A circuit breaker trip passes through as it is.
     */
    Batch expandBatch(Page parents, @Nullable int[] survivors, int rowGroupOrdinal, int pageBatchIndex, String fileLocation) {
        Page page;
        try {
            page = expand(parents, survivors, spec.errorPolicy().mode() == ErrorPolicy.Mode.SKIP_ROW);
        } catch (CircuitBreakingException e) {
            throw e;
        } catch (Exception e) {
            throw ParquetReadFailures.wrap(e, failureContext(rowGroupOrdinal, pageBatchIndex, fileLocation));
        }
        return new Batch(page, lastDroppedParents);
    }

    /**
     * The parents a {@code skip_row} drop keeps, of those the batch's earlier drops kept.
     *
     * @param batchPositions the kept parents' batch positions, ascending
     * @param pagePositions  their positions in the page of the parents the batch kept
     * @param dropped        how many parents the drop removed
     */
    private record KeptParents(int[] batchPositions, int[] pagePositions, int dropped) {}

    /**
     * The elements of a batch whose value of some leaf could not be converted, under {@code skip_row}. Its arrays, which no
     * block accounts for, are charged to the breaker until it is closed, as {@link ExpandedRows} charges its own.
     */
    private static final class FailedElements implements IntConsumer, Releasable {
        private final boolean[] failed;
        private final CircuitBreaker breaker;
        private long reserved;
        private boolean any;

        FailedElements(int elements, CircuitBreaker breaker) {
            this.breaker = breaker;
            reserve(elements);
            this.failed = new boolean[elements];
        }

        private void reserve(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, ExpandedRows.BREAKER_LABEL);
            reserved += bytes;
        }

        @Override
        public void accept(int element) {
            failed[element] = true;
            any = true;
        }

        boolean any() {
            return any;
        }

        /**
         * The parents that survive the batch's earlier drops, {@code survivors} or every one of its {@code rows} when that is
         * {@code null}, and have no element that failed to convert.
         */
        KeptParents keptParents(IntVector elementParents, @Nullable int[] survivors, int rows) {
            int parents = survivors == null ? rows : survivors.length;
            reserve(rows + 2L * parents * Integer.BYTES);
            boolean[] failedParent = new boolean[rows];
            for (int e = 0; e < elementParents.getPositionCount(); e++) {
                if (failed[e]) {
                    failedParent[elementParents.getInt(e)] = true;
                }
            }
            int[] batchPositions = new int[parents];
            int[] pagePositions = new int[parents];
            int k = 0;
            for (int p = 0; p < parents; p++) {
                int batchPosition = survivors == null ? p : survivors[p];
                if (failedParent[batchPosition] == false) {
                    batchPositions[k] = batchPosition;
                    pagePositions[k] = p;
                    k++;
                }
            }
            return new KeptParents(Arrays.copyOf(batchPositions, k), Arrays.copyOf(pagePositions, k), parents - k);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-reserved);
            reserved = 0;
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

    /**
     * Where a failure to decode or expand the elements of a batch happened, for its message: the same words from both
     * iterators, so a failure reads alike whichever one the read took.
     */
    String failureContext(int rowGroupOrdinal, int pageBatchIndex, String fileLocation) {
        return "Failed to expand ["
            + path()
            + "] at row group ["
            + (rowGroupOrdinal + 1)
            + "] page batch ["
            + pageBatchIndex
            + "] in file ["
            + fileLocation
            + "]";
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
        private final PrimitiveType primitive;
        private final BlockFactory blockFactory;

        private boolean recordsParents;
        /** Whether the batch dropped a null element of the collection; see {@link #announceNullElements}. */
        private boolean droppedNullElement;
        /** Whether the batch dropped a null value of a list inside the element. */
        private boolean droppedNullInnerValue;
        /** The definition level of a null value of a list inside the element, or {@code -1} when the leaf is in none. */
        private final int nullInnerValueDef;
        /** Whether the batch held a value out of its type's range, read as null; see {@link #appendValue}. */
        private boolean outOfRange;
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
            this.primitive = descriptor.getPrimitiveType();
            // A leaf repeated below the collection's own repetition is a list inside the element; an optional value of it
            // one level short of present is a null element of that list, which a list column announces when it drops it
            this.nullInnerValueDef = descriptor.getMaxRepetitionLevel() > collectionRep && primitive.isRepetition(Type.Repetition.OPTIONAL)
                ? maxDef - 1
                : -1;
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
         * count. The walk runs on a cursor that validates the levels and charges a malformed list to the read's error
         * budget.
         */
        int readRow(int parent) {
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
                        droppedNullElement = true;
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
                    if (open && emitsValues && def == nullInnerValueDef) {
                        droppedNullInnerValue = true;
                    }
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

        /** Decodes the value {@code reader} is on as a column of the leaf's type does; see {@link ParquetColumnDecoding}. */
        private void appendValue(ColumnReader reader) {
            if (fileType == DataType.DATE_NANOS) {
                // The column's range rule: a microsecond value with no nanosecond instant is skipped, and an element left
                // without a value is null (closeElement)
                LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
                long raw = reader.getLong();
                if (ParquetColumnDecoding.isMicrosTemporal(logical) && ParquetColumnDecoding.microsOverflowsNanos(raw)) {
                    outOfRange = true;
                    return;
                }
                beginValue();
                ((LongBlock.Builder) builder).appendLong(ParquetColumnDecoding.convertTemporalToNanos(raw, logical));
                return;
            }
            beginValue();
            ParquetColumnDecoding.appendInferredValue(reader, primitive, fileType, builder);
        }

        private void beginValue() {
            if (valuesInElement == 0) {
                builder.beginPositionEntry();
            }
            valuesInElement++;
        }

        private Block.Builder newBuilder() {
            int size = 16;
            return switch (fileType) {
                case BOOLEAN -> blockFactory.newBooleanBlockBuilder(size);
                case INTEGER -> blockFactory.newIntBlockBuilder(size);
                case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> blockFactory.newLongBlockBuilder(size);
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
        Block finishValues(Supplier<SkipWarnings> castWarnings, @Nullable IntConsumer failed, @Nullable Consumer<String> warningSink) {
            if (outOfRange) {
                outOfRange = false;
                // Named as the query names the column, not by the file's path through the list
                ParquetColumnDecoding.warnTimestampOutOfRange(columnName, warningSink);
            }
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
            outOfRange = false;
            droppedNullElement = false;
            droppedNullInnerValue = false;
            Releasables.closeExpectNoException(builder, parentBuilder);
            builder = null;
            parentBuilder = null;
        }
    }
}
