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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

/**
 * Load blocks with only {@code value}.
 */
public class ConstantBytes implements BlockLoader {
    private final Reader reader = new Reader();
    private final BytesRef value;

    public ConstantBytes(BytesRef value) {
        this.value = value;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return () -> reader;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) {
        return reader;
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
        return "ConstantBytes[" + value + "]";
    }

    private class Reader implements AllReader {
        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) {
            return factory.constantBytes(value, docs.count() - offset);
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) {
            ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(value);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public String toString() {
            return "constant[" + value + "]";
        }
    }
}
