/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Wraps a format reader's page iterator and runs each page through a {@link ColumnMapping}
 * so the file's local-schema pages emerge in the query's output shape. The mapping owns the
 * null-filling, casting, and bookkeeping; this iterator just drives the loop.
 */
final class SchemaAdaptingIterator implements CloseableIterator<Page> {

    private final CloseableIterator<Page> delegate;
    private final ColumnMapping mapping;
    private final BlockFactory blockFactory;

    SchemaAdaptingIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> outputSchema,
        ColumnMapping mapping,
        BlockFactory blockFactory
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
            return mapping.mapPage(filePage, blockFactory);
        } finally {
            filePage.releaseBlocks();
        }
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
