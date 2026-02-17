/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * A {@link BlockLoader.ColumnAtATimeReader} that immediately closes the reader after it is used.
 */
public record ColumnAtATimeReaderWithoutReuse(
    CircuitBreaker breaker,
    IOFunction<CircuitBreaker, BlockLoader.ColumnAtATimeReader> fn,
    Consumer<BlockLoader.Reader> track
) implements BlockLoader.ColumnAtATimeReader {
    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        try (BlockLoader.ColumnAtATimeReader reader = fn.apply(breaker)) {
            track.accept(reader);
            return reader.read(factory, docs, offset, nullsFiltered);
        }
    }

    @Override
    public boolean canReuse(int startingDocID) {
        // There's no state preserved to reuse
        return true;
    }

    @Override
    public void close() {}
}
