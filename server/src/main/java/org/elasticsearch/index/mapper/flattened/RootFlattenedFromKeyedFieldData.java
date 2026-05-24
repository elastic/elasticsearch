/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.FlattenedDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Field data implementation for the root flattened field on indices that no longer write
 * root doc values. Derives root values by reading the keyed doc values column and stripping
 * the key prefix from each entry, with deduplication.
 */
public final class RootFlattenedFromKeyedFieldData implements IndexFieldData<LeafFieldData> {

    private final IndexFieldData<?> keyedDelegate;
    private final String rootFieldName;

    RootFlattenedFromKeyedFieldData(String rootFieldName, IndexFieldData<?> keyedDelegate) {
        this.rootFieldName = rootFieldName;
        this.keyedDelegate = keyedDelegate;
    }

    @Override
    public String getFieldName() {
        return rootFieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public LeafFieldData load(LeafReaderContext context) {
        LeafFieldData keyedLeaf = keyedDelegate.load(context);
        return new RootFromKeyedLeafFieldData(keyedLeaf);
    }

    @Override
    public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        LeafFieldData keyedLeaf = keyedDelegate.loadDirect(context);
        return new RootFromKeyedLeafFieldData(keyedLeaf);
    }

    /**
     * Leaf field data that strips key prefixes from keyed doc values and deduplicates the result.
     */
    private static class RootFromKeyedLeafFieldData implements LeafFieldData {

        private final LeafFieldData keyedLeafData;

        RootFromKeyedLeafFieldData(LeafFieldData keyedLeafData) {
            this.keyedLeafData = keyedLeafData;
        }

        @Override
        public long ramBytesUsed() {
            return keyedLeafData.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return keyedLeafData.getChildResources();
        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            ToScriptFieldFactory<SortedBinaryDocValues> factory = FlattenedDocValuesField::new;
            return factory.getScriptFieldFactory(getBytesValues(), name);
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            SortedBinaryDocValues keyedValues = keyedLeafData.getBytesValues();
            return new KeyStrippingDeduplicatingDocValues(keyedValues);
        }
    }

    /**
     * Wraps keyed {@link SortedBinaryDocValues}, strips the key prefix (everything up to and
     * including the \0 separator) from each value, and deduplicates the results. The output
     * values are sorted lexicographically.
     */
    static final class KeyStrippingDeduplicatingDocValues extends SortedBinaryDocValues {

        private final SortedBinaryDocValues delegate;
        private final TreeSet<BytesRef> uniqueValues = new TreeSet<>();
        private Iterator<BytesRef> iterator;

        KeyStrippingDeduplicatingDocValues(SortedBinaryDocValues delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            uniqueValues.clear();

            if (delegate.advanceExact(target) == false) {
                return false;
            }

            int count = delegate.docValueCount();
            for (int i = 0; i < count; i++) {
                BytesRef keyedValue = delegate.nextValue();
                BytesRef stripped = FlattenedFieldParser.extractValue(keyedValue);
                uniqueValues.add(BytesRef.deepCopyOf(stripped));
            }

            iterator = uniqueValues.iterator();
            return uniqueValues.isEmpty() == false;
        }

        @Override
        public int docValueCount() {
            return uniqueValues.size();
        }

        @Override
        public BytesRef nextValue() throws IOException {
            return iterator.next();
        }
    }

    public static final class Builder implements IndexFieldData.Builder {
        private final String rootFieldName;
        private final String keyedFieldName;
        private final boolean usesBinaryDocValues;
        private final IndexVersion indexVersion;

        public Builder(String rootFieldName, String keyedFieldName, boolean usesBinaryDocValues, IndexVersion indexVersion) {
            this.rootFieldName = rootFieldName;
            this.keyedFieldName = keyedFieldName;
            this.usesBinaryDocValues = usesBinaryDocValues;
            this.indexVersion = indexVersion;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            IndexFieldData<?> keyedDelegate;
            if (usesBinaryDocValues) {
                keyedDelegate = new BytesBinaryIndexFieldData(
                    keyedFieldName,
                    CoreValuesSourceType.KEYWORD,
                    FlattenedDocValuesField::new,
                    indexVersion
                );
            } else {
                keyedDelegate = new SortedSetOrdinalsIndexFieldData(
                    cache,
                    keyedFieldName,
                    CoreValuesSourceType.KEYWORD,
                    breakerService,
                    (dv, n) -> {
                        throw new UnsupportedOperationException();
                    }
                );
            }
            return new RootFlattenedFromKeyedFieldData(rootFieldName, keyedDelegate);
        }
    }
}
