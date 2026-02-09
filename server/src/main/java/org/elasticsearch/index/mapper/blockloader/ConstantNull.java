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
 * Load blocks with only null.
 */
public class ConstantNull implements BlockLoader {
    public static final BlockLoader INSTANCE = new ConstantNull();
    public static final BlockLoader.AllReader READER = new Reader();

    private ConstantNull() {}

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.nulls(expectedCount);
    }

    @Override
    public IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return () -> READER;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) {
        return READER;
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ConstantNull";
    }

    /**
     * Implementation of {@link ColumnAtATimeReader} and {@link RowStrideReader} that always
     * loads {@code null}.
     */
    private static class Reader implements AllReader {
        private Reader() {}

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            return factory.constantNulls(docs.count() - offset);
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            builder.appendNull();
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public String toString() {
            return "constant_nulls";
        }
    }
}
