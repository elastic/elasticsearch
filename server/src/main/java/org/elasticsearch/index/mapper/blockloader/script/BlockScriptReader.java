/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * A reader that supports reading doc-values from a Lucene segment in Block fashion.
 */
public abstract class BlockScriptReader implements BlockLoader.RowStrideReader {
    protected final CircuitBreaker breaker;
    private final long byteSize;
    private final Thread creationThread;

    public BlockScriptReader(CircuitBreaker breaker, long byteSize) {
        this.breaker = breaker;
        this.byteSize = byteSize;
        this.creationThread = Thread.currentThread();
    }

    protected abstract int docId();

    /**
     * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
     */
    @Override
    public final boolean canReuse(int startingDocID) {
        return creationThread == Thread.currentThread() && docId() <= startingDocID;
    }

    @Override
    public final void close() {
        breaker.addWithoutBreaking(-byteSize);
    }

    @Override
    public abstract String toString();

    public abstract static class ScriptBlockLoader implements BlockLoader {
        private final long byteSize;

        protected ScriptBlockLoader(ByteSizeValue byteSize) {
            this.byteSize = byteSize.getBytes();
        }

        public abstract BlockScriptReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException;

        @Override
        public final IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
            return null;
        }

        @Override
        public final RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            breaker.addEstimateBytesAndMaybeBreak(byteSize, "load blocks");
            RowStrideReader reader = null;
            try {
                reader = reader(breaker, context);
                return reader;
            } finally {
                if (reader == null) {
                    breaker.addWithoutBreaking(-byteSize);
                }
            }
        }

        @Override
        public final StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }

        @Override
        public boolean supportsOrdinals() {
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
