/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

public class ConstantIndexFieldData extends AbstractIndexOrdinalsFieldData {

    public static class Builder implements IndexFieldData.Builder {

        private final String constantValue;
        private final String name;
        private final ValuesSourceType valuesSourceType;
        private final ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory;

        public Builder(
            String constantValue,
            String name,
            ValuesSourceType valuesSourceType,
            ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
        ) {
            this.constantValue = constantValue;
            this.name = name;
            this.valuesSourceType = valuesSourceType;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new ConstantIndexFieldData(name, constantValue, valuesSourceType, toScriptFieldFactory);
        }
    }

    private static class ConstantLeafFieldData extends AbstractLeafOrdinalsFieldData {

        private final String value;

        ConstantLeafFieldData(String value, ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory) {
            super(toScriptFieldFactory);
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            if (value == null) {
                return DocValues.emptySortedSet();
            }
            final BytesRef term = new BytesRef(value);
            final SortedDocValues sortedValues = new AbstractSortedDocValues() {

                private int docID = -1;

                @Override
                public BytesRef lookupOrd(int ord) {
                    return term;
                }

                @Override
                public int getValueCount() {
                    return 1;
                }

                @Override
                public int ordValue() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) {
                    docID = target;
                    return true;
                }

                @Override
                public int docID() {
                    return docID;
                }
            };
            return DocValues.singleton(sortedValues);
        }

        @Override
        public void close() {}

    }

    private final ConstantLeafFieldData atomicFieldData;

    private ConstantIndexFieldData(
        String name,
        String value,
        ValuesSourceType valuesSourceType,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
    ) {
        super(name, valuesSourceType, null, null, toScriptFieldFactory);
        atomicFieldData = new ConstantLeafFieldData(value, toScriptFieldFactory);
    }

    @Override
    public final LeafOrdinalsFieldData load(LeafReaderContext context) {
        return atomicFieldData;
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        return atomicFieldData;
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) {
        return loadGlobal(indexReader);
    }

    public String getValue() {
        return atomicFieldData.value;
    }

}
