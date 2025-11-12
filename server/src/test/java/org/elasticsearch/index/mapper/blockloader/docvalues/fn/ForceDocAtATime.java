/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.Supplier;

public class ForceDocAtATime implements BlockLoader.AllReader {
    private final Supplier<BlockLoader.Builder> builder;
    private final BlockLoader.AllReader delegate;

    public ForceDocAtATime(Supplier<BlockLoader.Builder> builder, BlockLoader.AllReader delegate) {
        this.builder = builder;
        this.delegate = delegate;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        try (BlockLoader.Builder builder = this.builder.get()) {
            for (int i = 0; i < docs.count(); i++) {
                delegate.read(docs.get(i), null, builder);
            }
            return builder.build();
        }
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        delegate.read(docId, storedFields, builder);
    }

    @Override
    public boolean canReuse(int startingDocID) {
        return delegate.canReuse(startingDocID);
    }
}
