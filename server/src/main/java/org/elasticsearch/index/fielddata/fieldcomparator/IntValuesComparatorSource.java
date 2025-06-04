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
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.IntComparator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Comparator source for integer values.
 */
public class IntValuesComparatorSource extends LongValuesComparatorSource {

    public IntValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        NumericType targetNumericType
    ) {
        super(indexFieldData, missingValue, sortMode, nested, null, targetNumericType);
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.INT;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final int iMissingValue = (Integer) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new IntComparator(numHits, fieldname, null, reversed, enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new IntLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return IntValuesComparatorSource.this.getNumericDocValues(context, iMissingValue);
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
        return new BucketedSort.ForInts(bigArrays, sortOrder, format, bucketSize, extra) {
            private final int iMissingValue = (Integer) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDocValues docValues = getNumericDocValues(ctx, iMissingValue);
                    private int docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = (int) docValues.longValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected int docValue() {
                        return docValue;
                    }
                };
            }
        };
    }

}
