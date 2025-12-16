/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * A reader that supports reading doc-values from a Lucene segment in Block fashion.
 */
public abstract class BlockDocValuesReader implements BlockLoader.AllReader {
    private final Thread creationThread;

    public BlockDocValuesReader() {
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
    public abstract String toString();

    public abstract static class DocValuesBlockLoader implements BlockLoader {
        public abstract AllReader reader(LeafReaderContext context) throws IOException;

        @Override
        public final ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            return reader(context);
        }

        @Override
        public final RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            return reader(context);
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

    // Used for testing.
    public interface NumericDocValuesAccessor {
        NumericDocValues numericDocValues();
    }

    /**
     * Convert from the stored {@link long} into the {@link double} to load.
     * Sadly, this will go megamorphic pretty quickly and slow us down,
     * but it gets the job done for now.
     */
    public interface ToDouble {
        double convert(long v);
    }
}
