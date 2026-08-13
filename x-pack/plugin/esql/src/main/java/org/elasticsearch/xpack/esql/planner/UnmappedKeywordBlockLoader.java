/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads a single unmapped field from {@code _source} as a keyword. ES|QL fabricates a keyword type for a field that is unmapped on a
 * shard (see {@code EsPhysicalOperationProviders.DefaultShardContextForUnmappedField}); when that path's {@code _source} value is an
 * object, a keyword scalar has no representation for it, so the field reads as {@code null}.
 */
final class UnmappedKeywordBlockLoader implements BlockLoader {

    private final String fieldName;

    UnmappedKeywordBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
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
        return new StoredFieldsSpec(true, false, Set.of());
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
        return "UnmappedKeywordBlockLoader[" + fieldName + "]";
    }

    private static final class Reader extends BlockStoredFieldsReader {
        private final String fieldName;

        Reader(CircuitBreaker breaker, String fieldName) {
            super(breaker);
            this.fieldName = fieldName;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            Source source = storedFields.source();
            List<BytesRef> values = new ArrayList<>();
            collectKeywordValues(source.extractValue(fieldName, null), values);
            if (values.isEmpty()) {
                builder.appendNull();
            } else if (values.size() == 1) {
                ((BytesRefBuilder) builder).appendBytesRef(values.get(0));
            } else {
                builder.beginPositionEntry();
                for (BytesRef value : values) {
                    ((BytesRefBuilder) builder).appendBytesRef(value);
                }
                builder.endPositionEntry();
            }
        }

        // A keyword scalar can only hold scalars, so objects (a Map) contribute no value and the field reads null; arrays keep only
        // their scalar elements.
        private static void collectKeywordValues(Object value, List<BytesRef> values) {
            if (value == null || value instanceof Map) {
                return;
            }
            if (value instanceof List<?> list) {
                for (Object element : list) {
                    collectKeywordValues(element, values);
                }
                return;
            }
            values.add(new BytesRef(value.toString()));
        }

        @Override
        public String toString() {
            return "UnmappedKeywordBlockLoader.Reader[" + fieldName + "]";
        }
    }
}
