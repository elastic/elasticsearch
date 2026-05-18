/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Wraps a format reader's page iterator to adapt file-local schema to the unified schema.
 * <p>
 * For each page produced by the delegate iterator, this adapter:
 * <ul>
 *   <li>Reorders columns to match the unified schema ordering</li>
 *   <li>Inserts constant NULL blocks for columns missing from this file</li>
 *   <li>Casts blocks to the unified type where safe widening was applied</li>
 * </ul>
 * <p>
 * This keeps format readers simple — they read only the columns their file has —
 * and centralizes NULL-filling and type-casting in one place.
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * The adapter unconditionally declares the {@link ColumnExtractorProducer} capability and forwards
 * {@link #createColumnExtractor()} / {@link #setExtractorId(int)} to its delegate. Whether those
 * calls actually succeed depends on the delegate: a non-producer delegate makes
 * {@code instanceof ColumnExtractorProducer} a necessary-but-not-sufficient guard at consumer
 * sites — the dispatch into the delegate fails loud (see {@link #innerProducer()}). Today the only
 * consumer is the deferred-extraction wiring in
 * {@code AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded}, which only reaches this
 * iterator when the factory has already arranged for {@code _rowPosition} (and therefore a
 * producer-capable delegate) on the read path; the unconditional declaration lets the dispatch
 * site stay a single {@code instanceof} check rather than threading a capability flag through the
 * adapter constructor.
 */
final class SchemaAdaptingIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private final CloseableIterator<Page> delegate;
    private final List<Attribute> unifiedSchema;
    private final SchemaReconciliation.ColumnMapping mapping;
    private final BlockFactory blockFactory;
    /**
     * Index in the delegate's input page of the synthetic
     * {@link ColumnExtractor#ROW_POSITION_COLUMN}, or {@code -1} when the file is not emitting it.
     * When non-negative, the adapter copies the block as-is into the trailing slot of the output
     * page so deferred extraction continues to work after schema reconciliation.
     */
    private final int rowPositionInputIndex;

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> unifiedSchema,
        SchemaReconciliation.ColumnMapping mapping,
        BlockFactory blockFactory
    ) {
        this(delegate, unifiedSchema, mapping, blockFactory, -1);
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> unifiedSchema,
        SchemaReconciliation.ColumnMapping mapping,
        BlockFactory blockFactory,
        int rowPositionInputIndex
    ) {
        if (unifiedSchema.size() != mapping.columnCount()) {
            throw new IllegalArgumentException(
                "Schema size ["
                    + unifiedSchema.size()
                    + "] does not match mapping column count ["
                    + mapping.columnCount()
                    + "]; callers must pass only data columns"
                    + " (use attributes.subList(0, mapping.columnCount()) to exclude partition columns)"
            );
        }
        this.delegate = delegate;
        this.unifiedSchema = unifiedSchema;
        this.mapping = mapping;
        this.blockFactory = blockFactory;
        this.rowPositionInputIndex = rowPositionInputIndex;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        if (delegate.hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page filePage = delegate.next();
        try {
            int positions = filePage.getPositionCount();
            int unifiedSize = unifiedSchema.size();
            int outputSize = unifiedSize + (rowPositionInputIndex >= 0 ? 1 : 0);

            Block[] unifiedBlocks = new Block[outputSize];
            try {
                for (int i = 0; i < unifiedSize; i++) {
                    int localIndex = mapping.localIndex(i);
                    if (localIndex == -1) {
                        unifiedBlocks[i] = blockFactory.newConstantNullBlock(positions);
                    } else {
                        Block block = filePage.getBlock(localIndex);
                        DataType castTo = mapping.cast(i);
                        if (castTo != null) {
                            unifiedBlocks[i] = castBlock(block, castTo, positions);
                        } else {
                            block.incRef();
                            unifiedBlocks[i] = block;
                        }
                    }
                }
                if (rowPositionInputIndex >= 0) {
                    Block rowPos = filePage.getBlock(rowPositionInputIndex);
                    rowPos.incRef();
                    unifiedBlocks[unifiedSize] = rowPos;
                }
                return new Page(positions, unifiedBlocks);
            } catch (Exception e) {
                Releasables.closeExpectNoException(unifiedBlocks);
                throw new RuntimeException("Failed to adapt page to unified schema", e);
            }
        } finally {
            filePage.releaseBlocks();
        }
    }

    @Override
    public ColumnExtractor createColumnExtractor() throws IOException {
        return innerProducer().createColumnExtractor();
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

    private Block castBlock(Block source, DataType targetType, int positions) {
        if (source instanceof IntBlock intBlock) {
            if (targetType == DataType.LONG) {
                return castIntToLong(intBlock, positions);
            } else if (targetType == DataType.DOUBLE) {
                return castIntToDouble(intBlock, positions);
            }
        } else if (source instanceof LongBlock longBlock && targetType == DataType.DATE_NANOS) {
            return castDatetimeToDateNanos(longBlock, positions);
        }
        throw new UnsupportedOperationException(
            "Unsupported block cast: " + source.getClass().getSimpleName() + " → " + targetType.typeName()
        );
    }

    private Block castIntToLong(IntBlock intBlock, int positions) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = intBlock.getValueCount(pos);
                if (intBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendLong(intBlock.getInt(intBlock.getFirstValueIndex(pos)));
                } else {
                    int firstIdx = intBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(intBlock.getInt(firstIdx + v));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private Block castIntToDouble(IntBlock intBlock, int positions) {
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = intBlock.getValueCount(pos);
                if (intBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendDouble(intBlock.getInt(intBlock.getFirstValueIndex(pos)));
                } else {
                    int firstIdx = intBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendDouble(intBlock.getInt(firstIdx + v));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private Block castDatetimeToDateNanos(LongBlock longBlock, int positions) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = longBlock.getValueCount(pos);
                if (longBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendLong(longBlock.getLong(longBlock.getFirstValueIndex(pos)) * 1_000_000L);
                } else {
                    int firstIdx = longBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(longBlock.getLong(firstIdx + v) * 1_000_000L);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
