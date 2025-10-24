/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.KnnVectorValues;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Abstract base class for readers that process dense vector values.
 * Provides common iteration and null-handling logic.
 *
 * @param <T> The type of KNN vector values (FloatVectorValues, ByteVectorValues, etc.)
 * @param <B> The type of builder used to construct blocks
 */
public abstract class AbstractVectorValuesReader<T extends KnnVectorValues, B extends BlockLoader.Builder> extends
    BlockDocValuesReader {

    protected final T vectorValues;
    protected final KnnVectorValues.DocIndexIterator iterator;
    protected final VectorProcessor<B> processor;
    protected final int dimensions;

    protected AbstractVectorValuesReader(T vectorValues, VectorProcessor<B> processor, int dimensions) {
        this.vectorValues = vectorValues;
        this.iterator = vectorValues.iterator();
        this.processor = processor;
        this.dimensions = dimensions;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        try (B builder = processor.createBuilder(factory, docs.count() - offset, dimensions)) {
            for (int i = offset; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        read(docId, (B) builder);
    }

    /**
     * Reads a document and appends it to the builder.
     * Handles null values when the document doesn't have a vector.
     */
    protected void read(int doc, B builder) throws IOException {
        if (iterator.docID() > doc) {
            processor.appendNull(builder);
        } else if (iterator.docID() == doc || iterator.advance(doc) == doc) {
            processCurrentVector(builder);
        } else {
            processor.appendNull(builder);
        }
    }

    @Override
    public int docId() {
        return iterator.docID();
    }

    /**
     * Retrieves the vector value at the current iterator position and processes it.
     * Subclasses must call the appropriate processor.process() method with the correct type.
     */
    protected abstract void processCurrentVector(B builder) throws IOException;

    protected void assertDimensions() {
        assert vectorValues.dimension() == dimensions
            : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension();
    }
}
