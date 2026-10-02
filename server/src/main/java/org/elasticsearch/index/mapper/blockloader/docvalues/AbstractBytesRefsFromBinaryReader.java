/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

/**
 * Shared base for the binary-family readers.
 */
public abstract class AbstractBytesRefsFromBinaryReader extends BlockDocValuesReader implements BlockLoader.RowStrideReader {

    protected final TrackingBinaryDocValues docValues;

    public AbstractBytesRefsFromBinaryReader(TrackingBinaryDocValues docValues) {
        super(null);
        this.docValues = docValues;
    }

    /**
     * Column-at-a-time read: loads all docs in one pass and returns a fully-built {@link BlockLoader.Block} for the batch.
     */
    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
            for (int i = offset; i < docs.count(); i++) {
                int doc = docs.get(i);
                read(doc, builder);
            }
            return builder.build();
        }
    }

    /**
     * Row-stride read: loads a single doc into an existing builder, used when the engine processes documents one at a time.
     */
    @Override
    public final void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        read(docId, (BlockLoader.BytesRefBuilder) builder);
    }

    /**
     * Per-doc primitive: decodes the binary blob for {@code docId} and appends the resulting value(s) to {@code builder}.
     */
    public abstract void read(int docId, BlockLoader.BytesRefBuilder builder) throws IOException;

    @Override
    public int docId() {
        return docValues.docValues().docID();
    }

    @Override
    public void close() {
        docValues.close();
    }
}
