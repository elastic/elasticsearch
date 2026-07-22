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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Block loader for the synthetic {@code _unmapped_fields} column produced by
 * {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>For each document it reads {@code _source}, retains only top-level keys
 * that match the {@link UnmappedFieldsPattern} (matching all include patterns and
 * not matching any exclude pattern), and re-serialises the surviving key/value
 * pairs as a JSON object.
 *
 * <p><b>Known limitations</b> — this is proof-of-concept quality and needs hardening before it
 * could ship: it does not apply field-level security to the {@code _source} keys it exposes; it
 * re-reads and re-parses {@code _source} per document instead of sharing a cached parse with the
 * other field-extraction operators that also touch {@code _source} (partially mapped fields and
 * demand-loaded unmapped fields); and it does not account its allocations against the
 * {@link CircuitBreaker}.
 */
final class UnmappedFieldsBlockLoader implements BlockLoader {

    private final UnmappedFieldsPattern pattern;

    UnmappedFieldsBlockLoader(UnmappedFieldsPattern pattern) {
        this.pattern = pattern;
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
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return new UnmappedFields(breaker, pattern);
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
        return "UnmappedFieldsBlockLoader";
    }

    private static class UnmappedFields extends BlockStoredFieldsReader {
        private final UnmappedFieldsPattern pattern;

        UnmappedFields(CircuitBreaker breaker, UnmappedFieldsPattern pattern) {
            super(breaker);
            this.pattern = pattern;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            Source source = storedFields.source();
            Map<String, Object> sourceMap = XContentHelper.convertToMap(source.internalSourceRef(), false, source.sourceContentType()).v2();
            TreeMap<String, Object> filtered = new TreeMap<>();
            for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                if (included(entry.getKey())) {
                    filtered.put(entry.getKey(), entry.getValue());
                }
            }
            try (XContentBuilder json = XContentFactory.jsonBuilder()) {
                json.map(filtered);
                ((BytesRefBuilder) builder).appendBytesRef(BytesReference.bytes(json).toBytesRef());
            }
        }

        private boolean included(String fieldName) {
            if (pattern.includes().isEmpty()) {
                return false;
            }
            for (String exclude : pattern.excludes()) {
                if (Regex.simpleMatch(exclude, fieldName)) {
                    return false;
                }
            }
            for (String include : pattern.includes()) {
                if (Regex.simpleMatch(include, fieldName) == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return "UnmappedFieldsBlockLoader";
        }
    }
}
