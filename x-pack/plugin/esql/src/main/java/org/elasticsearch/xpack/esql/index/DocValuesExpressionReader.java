/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.List;

/**
 * Evaluates an expression on the documents of a segment, a chunk of consecutive documents at a time: the fields' block loaders
 * read each chunk a column at a time, and the evaluator turns the page into the chunk's result. Reads forward only.
 */
final class DocValuesExpressionReader implements Releasable {
    static final int CHUNK = 1024;

    private static final CircuitBreaker BREAKER = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

    private final BlockLoader.ColumnAtATimeReader[] readers;
    private final ExpressionEvaluator evaluator;
    private final BlockLoader.BlockFactory loaderFactory;
    private final int maxDoc;
    private final ChunkDocs docs = new ChunkDocs();
    private boolean closed;

    DocValuesExpressionReader(
        LeafReaderContext context,
        int maxDoc,
        List<BlockLoader> loaders,
        ExpressionEvaluator evaluator,
        BlockLoader.BlockFactory loaderFactory
    ) throws IOException {
        this.readers = new BlockLoader.ColumnAtATimeReader[loaders.size()];
        this.evaluator = evaluator;
        this.loaderFactory = loaderFactory;
        this.maxDoc = maxDoc;
        boolean success = false;
        try {
            for (int i = 0; i < readers.length; i++) {
                var reader = loaders.get(i).columnAtATimeReader(context);
                if (reader == null) {
                    throw new IllegalStateException("[" + loaders.get(i) + "] can't read a column at a time");
                }
                readers[i] = reader.apply(BREAKER);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Evaluates the next chunk, whose positions are the documents from {@link #firstDoc()}, or returns {@code null} once every
     * document is read. Closes this reader with the last chunk. The caller releases the block.
     */
    @Nullable
    Block next() throws IOException {
        int first = docs.first + docs.count;
        if (closed || first >= maxDoc) {
            close();
            return null;
        }
        docs.first = first;
        docs.count = Math.min(CHUNK, maxDoc - first);
        Block[] blocks = new Block[readers.length];
        try {
            for (int i = 0; i < readers.length; i++) {
                blocks[i] = (Block) readers[i].read(loaderFactory, docs, 0, false);
            }
            Block result = evaluator.eval(new Page(docs.count, blocks));
            // Lucene stops at the last document, so this is the last chance to close
            if (first + docs.count == maxDoc) {
                close();
            }
            return result;
        } finally {
            Releasables.close(blocks);
        }
    }

    /** The first document of the last chunk. */
    int firstDoc() {
        return docs.first;
    }

    long cost() {
        return maxDoc;
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Releasables.close(Releasables.wrap(readers), evaluator);
        }
    }

    /** The consecutive documents of a chunk. */
    private static final class ChunkDocs implements BlockLoader.Docs {
        private int first;
        private int count;

        @Override
        public int count() {
            return count;
        }

        @Override
        public int get(int i) {
            return first + i;
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }
}
