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
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Set;

/**
 * Reads a field whose type has no implicit conversion from {@code KEYWORD}, so it cannot be reconstructed from {@code _source}. A
 * document without the field reads as null; one that carries a value fails the query rather than dropping the value silently.
 */
final class UnmappedNonLoadableBlockLoader implements BlockLoader {
    private final String fieldName;
    private final DataType dataType;
    private final Set<String> sourcePaths;
    private final IgnoredSourceFormat ignoredSourceFormat;

    UnmappedNonLoadableBlockLoader(String fieldName, DataType dataType, Set<String> sourcePaths, IgnoredSourceFormat ignoredSourceFormat) {
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.sourcePaths = sourcePaths;
        this.ignoredSourceFormat = ignoredSourceFormat;
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
        return new Reader(breaker, fieldName, dataType);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.withSourcePaths(ignoredSourceFormat, sourcePaths);
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
        private final DataType dataType;

        Reader(CircuitBreaker breaker, String fieldName, DataType dataType) {
            super(breaker);
            this.fieldName = fieldName;
            this.dataType = dataType;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (storedFields.source().extractValue(fieldName, null) != null) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Field [%s] of type [%s] is unmapped in this index and has no implicit conversion from KEYWORD, "
                            + "so its _source value cannot be loaded",
                        fieldName,
                        dataType.typeName()
                    )
                );
            }
            builder.appendNull();
        }

        @Override
        public String toString() {
            return "UnmappedNonLoadableBlockLoader.Reader[" + fieldName + "]";
        }
    }
}
