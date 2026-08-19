/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Wraps a format reader's page iterator and runs each page through a {@link ColumnMapping}
 * so the file's local-schema pages emerge in the query's output shape. {@link ColumnMapping}
 * owns the null-filling and casting; this iterator drives the loop and (when configured)
 * appends a synthetic {@code _rowPosition} block in the trailing slot so deferred extraction
 * continues to work after schema reconciliation.
 *
 * <h2>{@link BlockFactory} threading contract</h2>
 * This iterator runs on the producer side of {@link AsyncExternalSourceBuffer} (the generic /
 * external-source pool thread that drains pages from the format reader), <em>not</em> on the
 * driver thread that later consumes them. Callers must therefore pass a {@link BlockFactory}
 * that is safe to use off-driver — typically the node-level (root) factory wired via
 * {@link AsyncExternalSourceOperatorFactory.Builder#producerBlockFactory(BlockFactory)} — so the
 * null-fill and cast allocations are charged to the global request circuit breaker. Passing a
 * driver-local factory backed by {@link org.elasticsearch.compute.data.LocalCircuitBreaker}
 * trips its single-thread assertion in debug builds and races with the driver loop's
 * reserved-bytes accounting in production (assertions stripped); see
 * {@link VirtualColumnIterator} for the same contract on the {@code _file.*} path.
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * The adapter unconditionally declares the {@link ColumnExtractorProducer} capability and forwards
 * {@link #createColumnExtractor(Consumer)} / {@link #setExtractorId(int)} to its delegate. Whether those
 * calls actually succeed depends on the delegate: a non-producer delegate makes
 * {@code instanceof ColumnExtractorProducer} a necessary-but-not-sufficient guard at consumer
 * sites — the dispatch into the delegate fails loud (see {@link #innerProducer()}). The only
 * consumer is the deferred-extraction wiring in
 * {@code AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded}, which only reaches this
 * iterator when the factory has already arranged for {@code _rowPosition} (and therefore a
 * producer-capable delegate) on the read path.
 */
final class SchemaAdaptingIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private final CloseableIterator<Page> delegate;
    private final ColumnMapping mapping;
    private final BlockFactory blockFactory;
    /**
     * Index in the delegate's input page of the synthetic
     * {@link ColumnExtractor#ROW_POSITION_COLUMN}, or {@code -1} when the file is not emitting it.
     * When non-negative, the adapter copies the block as-is into the trailing slot of the output
     * page so deferred extraction continues to work after schema reconciliation.
     */
    private final int rowPositionInputIndex;
    /**
     * File-side ES|QL type per reader-emitted block, in reader-natural (projected) order, or
     * {@code null} when no caller cares to disambiguate. Used exclusively by
     * {@link ColumnMapping#mapPage(Page, BlockFactory, DataType[], String[], SkipWarnings)} to
     * tell apart {@code LongBlock} sources (DATETIME / DATE_NANOS / LONG share one block class)
     * when casting to KEYWORD.
     */
    @Nullable
    private final DataType[] perFileColumnTypes;
    /** Output column name per mapping slot; names the column in per-value cast warnings. */
    private final String[] outputColumnNames;
    /**
     * Expected element type per output slot, derived from the output schema at construction.
     * Used to detect block-type mismatches (e.g. {@code IntBlock} where the plan expects
     * {@code LongBlock}) before they reach a consumer that casts and throws a bare
     * {@link ClassCastException} naming no column.
     */
    private final ElementType[] expectedElementTypes;
    /**
     * Lazily-created sink for per-value reconciliation-cast failures, shared by every page of
     * this iterator so the warning cap is per file read. Mirrors the readers' declared-coercion
     * sinks (e.g. {@code ParquetColumnIterator#coercionWarnings()}) so a value that cannot be
     * represented at the unified type warns and nulls identically to one that cannot be coerced
     * to a declared type.
     */
    private SkipWarnings castWarnings;
    /**
     * Deferred sink for absent-declared-column informational warnings. Non-null when the mapping
     * has at least one {@code -1} slot that has not yet been warned about. Emitted lazily on the
     * first call to {@link #adaptPage} so that splits whose row groups are pruned to zero rows by
     * predicate statistics (no data returned) do not trigger warnings for a column that never
     * affected the result. Set to {@code null} once warnings have been emitted.
     */
    @Nullable
    private Consumer<String> pendingAbsentColumnWarningSink;
    /**
     * Output column names for slots with a {@code -1} mapping (absent from the file) that warrant
     * a user-visible informational warning. Computed at construction alongside
     * {@link #pendingAbsentColumnWarningSink}; emptied after warnings are emitted.
     */
    @Nullable
    private String[] pendingAbsentColumnNames;

    private SkipWarnings castWarnings() {
        if (castWarnings == null) {
            castWarnings = new SkipWarnings(
                "Cross-file schema unification could not convert some values to the unified column type; they are returned as null"
            );
        }
        return castWarnings;
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory
    ) {
        this(delegate, outputSchema, mapping, blockFactory, -1, null);
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory,
        int rowPositionInputIndex
    ) {
        this(delegate, outputSchema, mapping, blockFactory, rowPositionInputIndex, null);
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory,
        int rowPositionInputIndex,
        @Nullable DataType[] perFileColumnTypes
    ) {
        this(delegate, outputSchema, mapping, blockFactory, rowPositionInputIndex, perFileColumnTypes, null);
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory,
        int rowPositionInputIndex,
        @Nullable DataType[] perFileColumnTypes,
        @Nullable Consumer<String> informationalWarningSink
    ) {
        if (outputSchema.size() != mapping.width()) {
            throw new IllegalArgumentException(
                "output schema size ["
                    + outputSchema.size()
                    + "] does not match mapping width ["
                    + mapping.width()
                    + "]; callers must narrow attributes to data columns only"
                    + " (exclude partition columns before constructing this iterator)"
            );
        }
        this.delegate = delegate;
        this.mapping = mapping;
        this.blockFactory = blockFactory;
        this.rowPositionInputIndex = rowPositionInputIndex;
        this.perFileColumnTypes = perFileColumnTypes;
        this.outputColumnNames = outputSchema.stream().map(Attribute::name).toArray(String[]::new);
        this.expectedElementTypes = outputSchema.stream()
            .map(a -> DeclaredTypeCoercions.elementTypeFor(a.dataType()))
            .toArray(ElementType[]::new);
        // Defer absent-column warnings to first adaptPage: splits whose row groups are entirely
        // pruned by predicate statistics return zero rows, so warning at construction time would
        // mislead the user about a column that never affected the result.
        if (informationalWarningSink != null) {
            List<String> toWarn = null;
            for (int i = 0; i < mapping.width(); i++) {
                if (mapping.localIndex(i) == -1) {
                    DataType dt = outputSchema.get(i).dataType();
                    if (dt != DataType.NULL && dt != DataType.UNSUPPORTED) {
                        if (toWarn == null) {
                            toWarn = new ArrayList<>();
                        }
                        toWarn.add(outputColumnNames[i]);
                    }
                }
            }
            if (toWarn != null) {
                this.pendingAbsentColumnNames = toWarn.toArray(String[]::new);
                this.pendingAbsentColumnWarningSink = informationalWarningSink;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    /**
     * Forward the async-ready signal so the producer-loop drain parks (rather than blocking in a
     * parser-backed {@code hasNext()}) across the inter-chunk gap. Without this the default
     * immediately-done {@link CloseableIterator#waitForReady()} would swallow the delegate's real
     * signal — the multi-file text-read deadlock.
     */
    @Override
    public SubscribableListener<Void> waitForReady() {
        return delegate.waitForReady();
    }

    @Override
    public Page tryAdvance() {
        Page filePage = delegate.tryAdvance();
        return filePage != null ? adaptPage(filePage) : null;
    }

    @Override
    public Page next() {
        if (delegate.hasNext() == false) {
            throw new NoSuchElementException();
        }
        return adaptPage(delegate.next());
    }

    private Page adaptPage(Page filePage) {
        // Fast path: identity mapping with no row-position channel to append — the file page is
        // already in the correct shape. Validate types in-place and return without mapPage's
        // per-block incRef/alloc overhead (O(columns) atomics saved per page on wide schemas).
        // emitAbsentColumnWarningsOnce is inside the guard so filePage is released if it throws.
        if (mapping.isIdentity() && rowPositionInputIndex < 0) {
            try {
                validateOutputTypes(filePage);
                // Emit only after validation: if the page has wrong types the query fails hard;
                // emitting an absent-column warning alongside a fatal error implies partial success.
                emitAbsentColumnWarningsOnce();
            } catch (Exception e) {
                filePage.releaseBlocks();
                throw e;
            }
            return filePage;
        }
        // Non-identity path: filePage is always released in the outer finally regardless of
        // which inner operation throws. schemaAdapted is tracked via schemaAdaptedReleased to
        // avoid a double-release when it is decomposed into withRowPos before new Page() throws.
        try {
            Page schemaAdapted = mapping.mapPage(filePage, blockFactory, perFileColumnTypes, outputColumnNames, castWarnings());
            try {
                validateOutputTypes(schemaAdapted);
                // Emit only after validation: if the page has wrong types the query fails hard;
                // emitting an absent-column warning alongside a fatal error implies partial success.
                emitAbsentColumnWarningsOnce();
            } catch (Exception e) {
                schemaAdapted.releaseBlocks();
                throw e;
            }
            if (rowPositionInputIndex < 0) {
                return schemaAdapted;
            }
            int width = schemaAdapted.getBlockCount();
            Block[] withRowPos = new Block[width + 1];
            boolean schemaAdaptedReleased = false;
            try {
                for (int i = 0; i < width; i++) {
                    Block b = schemaAdapted.getBlock(i);
                    b.incRef();
                    withRowPos[i] = b;
                }
                Block rowPos = filePage.getBlock(rowPositionInputIndex);
                rowPos.incRef();
                withRowPos[width] = rowPos;
                int positions = schemaAdapted.getPositionCount();
                schemaAdapted.releaseBlocks();
                schemaAdaptedReleased = true;
                return new Page(positions, withRowPos);
            } catch (Throwable e) {
                Releasables.closeExpectNoException(withRowPos);
                if (schemaAdaptedReleased == false) {
                    schemaAdapted.releaseBlocks();
                }
                if (e instanceof RuntimeException r) {
                    throw r;
                }
                throw new RuntimeException("Failed to adapt page", e);
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    private void emitAbsentColumnWarningsOnce() {
        Consumer<String> sink = pendingAbsentColumnWarningSink;
        if (sink == null) {
            return;
        }
        pendingAbsentColumnWarningSink = null;
        String[] names = pendingAbsentColumnNames;
        pendingAbsentColumnNames = null;
        for (String name : names) {
            sink.accept(SkipWarnings.absentDeclaredColumnMessage(name));
        }
    }

    /**
     * Validates that every output block's element type matches the plan's declared type.
     * {@link ElementType#NULL} (constant-null block) is exempt — it is valid for any declared type
     * and is the normal representation of a missing column or an all-null column.
     * The row-position channel is not part of {@code schemaAdapted} (it is appended after), so no
     * special exemption is needed for it here.
     */
    private void validateOutputTypes(Page schemaAdapted) {
        if (schemaAdapted.getBlockCount() != expectedElementTypes.length) {
            throw new IllegalStateException(
                "mapping produced ["
                    + schemaAdapted.getBlockCount()
                    + "] blocks but plan expects ["
                    + expectedElementTypes.length
                    + "] columns"
            );
        }
        for (int i = 0; i < expectedElementTypes.length; i++) {
            ElementType actual = schemaAdapted.getBlock(i).elementType();
            if (actual != ElementType.NULL && actual != expectedElementTypes[i]) {
                throw new IllegalStateException(
                    "column ["
                        + outputColumnNames[i]
                        + "]: reader emitted block of element type ["
                        + actual
                        + "] but plan expects ["
                        + expectedElementTypes[i]
                        + "]"
                );
            }
        }
    }

    @Override
    public ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) throws IOException {
        return innerProducer().createColumnExtractor(driverThreadWarningSink);
    }

    @Override
    public void setExtractorId(int id) {
        innerProducer().setExtractorId(id);
    }

    /**
     * Pass-through capability: the producer is the inner iterator that owns the row-group scope
     * and pre-encodes {@code _rowPosition}. If the wrapped iterator is not a producer, deferred
     * extraction was wired to a reader that does not support it — fail loudly so callers don't
     * silently lose data.
     */
    private ColumnExtractorProducer innerProducer() {
        if (delegate instanceof ColumnExtractorProducer producer) {
            return producer;
        }
        throw new IllegalStateException(
            "deferred extraction requested but underlying iterator [" + delegate.getClass().getName() + "] is not a ColumnExtractorProducer"
        );
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close delegate iterator", e);
        }
    }
}
