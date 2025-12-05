/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.DenseLongValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.lucene.comparators.XLongComparator;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.Function;

/**
 * Comparator source for long values.
 */
public class LongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    final IndexNumericFieldData indexFieldData;
    private final Function<SortedNumericLongValues, SortedNumericLongValues> converter;
    private final NumericType targetNumericType;

    public LongValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        NumericType targetNumericType
    ) {
        this(indexFieldData, missingValue, sortMode, nested, null, targetNumericType);
    }

    public LongValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        Function<SortedNumericLongValues, SortedNumericLongValues> converter,
        NumericType targetNumericType
    ) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
        this.converter = converter;
        this.targetNumericType = targetNumericType;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    private SortedNumericLongValues loadDocValues(LeafReaderContext context) {
        final LeafNumericFieldData data = indexFieldData.load(context);
        SortedNumericLongValues values;
        if (data instanceof SortedNumericIndexFieldData.NanoSecondFieldData) {
            values = ((SortedNumericIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
        } else {
            values = data.getLongValues();
        }
        return converter != null ? converter.apply(values) : values;
    }

    DenseLongValues getLongValues(LeafReaderContext context, long missingValue) throws IOException {
        final SortedNumericLongValues values = loadDocValues(context);
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        }
        final BitSet rootDocs = nested.rootDocs(context);
        final DocIdSetIterator innerDocs = nested.innerDocs(context);
        final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
        return sortMode.select(values, missingValue, rootDocs, innerDocs, maxChildren);
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final long lMissingValue = (Long) missingObject(missingValue, reversed);
        return new XLongComparator(numHits, fieldname, lMissingValue, reversed, enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                return new LongLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return wrap(getLongValues(context, lMissingValue), maxDoc);
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return new BucketedSort.ForLongs(bigArrays, sortOrder, format, bucketSize, extra) {
            private final long lMissingValue = (Long) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final LongValues docValues = getLongValues(ctx, lMissingValue);
                    private long docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.longValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected long docValue() {
                        return docValue;
                    }
                };
            }
        };
    }

    @Override
    public Object missingObject(Object missingValue, boolean reversed) {
        if (targetNumericType == NumericType.DATE_NANOSECONDS) {
            // special case to prevent negative values that would cause invalid nanosecond ranges
            if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                final boolean min = sortMissingFirst(missingValue) ^ reversed;
                return min ? 0L : DateUtils.MAX_NANOSECOND;
            }
        }
        return super.missingObject(missingValue, reversed);
    }

    protected static NumericDocValues wrap(DenseLongValues longValues, int maxDoc) {
        return new NumericDocValues() {

            int doc = -1;

            @Override
            public long longValue() throws IOException {
                return longValues.longValue();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return longValues.advanceExact(target);
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                if (target >= maxDoc) {
                    return doc = NO_MORE_DOCS;
                }
                // All documents are guaranteed to have a value, as all invocations of getLongValues
                // always return `true` from `advanceExact()`
                boolean hasValue = longValues.advanceExact(target);
                assert hasValue : "LongValuesComparatorSource#wrap called with a LongValues that has missing values";
                doc = target;
                return target;
            }

            @Override
            public long cost() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
