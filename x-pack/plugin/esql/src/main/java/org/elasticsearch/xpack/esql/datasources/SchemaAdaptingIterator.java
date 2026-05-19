/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Wraps a format reader's page iterator and runs each page through a {@link ColumnMapping}
 * so the file's local-schema pages emerge in the query's output shape. {@link ColumnMapping}
 * owns the null-filling and casting; this iterator drives the loop and (when configured)
 * appends a synthetic {@code _rowPosition} block in the trailing slot so deferred extraction
 * continues to work after schema reconciliation.
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * The adapter unconditionally declares the {@link ColumnExtractorProducer} capability and forwards
 * {@link #createColumnExtractor()} / {@link #setExtractorId(int)} to its delegate. Whether those
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

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory
    ) {
        this(delegate, outputSchema, mapping, blockFactory, -1);
    }

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory,
        int rowPositionInputIndex
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
            Page schemaAdapted = mapping.mapPage(filePage, blockFactory);
            if (rowPositionInputIndex < 0) {
                return schemaAdapted;
            }
            // Extend the schema-adapted page with the row-position block in the trailing slot.
            int width = schemaAdapted.getBlockCount();
            Block[] withRowPos = new Block[width + 1];
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
                return new Page(positions, withRowPos);
            } catch (Exception e) {
                Releasables.closeExpectNoException(withRowPos);
                schemaAdapted.releaseBlocks();
                throw new RuntimeException("Failed to adapt page", e);
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
}
