/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * Reads a field whose type has no implicit conversion from {@code KEYWORD}, so it cannot be reconstructed from {@code _source}.
 * Every document reads as null; the analyzer warns that unmapped indices will not load the value.
 */
final class UnmappedNonLoadableBlockLoader implements BlockLoader {
    private final String fieldName;

    UnmappedNonLoadableBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.nulls(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
        return new Reader(breaker, fieldName);
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
        return "UnmappedNonLoadableBlockLoader[" + fieldName + "]";
    }

    private static final class Reader extends BlockStoredFieldsReader {
        private final String fieldName;

        Reader(CircuitBreaker breaker, String fieldName) {
            super(breaker);
            this.fieldName = fieldName;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            builder.appendNull();
        }

        @Override
        public String toString() {
            return "UnmappedNonLoadableBlockLoader.Reader[" + fieldName + "]";
        }
    }
}
