/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * Delegates to other {@link BlockLoader}.
 */
public abstract class DelegatingBlockLoader implements BlockLoader {
    protected final BlockLoader delegate;

    protected DelegatingBlockLoader(BlockLoader delegate) {
        this.delegate = delegate;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return delegate.builder(factory, expectedCount);
    }

    @Override
    public IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) throws IOException {
        IOSupplier<ColumnAtATimeReader> reader = delegate.columnAtATimeReader(context);
        if (reader == null) {
            return null;
        }
        return () -> new ColumnReader(reader.get());
    }

    private class ColumnReader implements ColumnAtATimeReader {
        private final ColumnAtATimeReader reader;

        ColumnReader(ColumnAtATimeReader reader) {
            this.reader = reader;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            return reader.read(factory, docs, offset, nullsFiltered);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return reader.canReuse(startingDocID);
        }

        @Override
        public String toString() {
            return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
        }
    }

    private class RowReader implements RowStrideReader {
        private final RowStrideReader reader;

        private RowReader(RowStrideReader reader) {
            this.reader = reader;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            reader.read(docId, storedFields, builder);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return reader.canReuse(startingDocID);
        }

        @Override
        public String toString() {
            return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
        }
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        RowStrideReader reader = delegate.rowStrideReader(context);
        if (reader == null) {
            return null;
        }
        return new RowReader(reader);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return delegate.rowStrideStoredFieldSpec();
    }

    @Override
    public boolean supportsOrdinals() {
        return delegate.supportsOrdinals();
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return delegate.ordinals(context);
    }

    public abstract String delegatingTo();

    @Override
    public final String toString() {
        return "Delegating[to=" + delegatingTo() + ", impl=" + delegate + "]";
    }
}
