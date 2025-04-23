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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for half_float values.
 */
public class HalfFloatValuesComparatorSource extends FloatValuesComparatorSource {
    public HalfFloatValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(indexFieldData, missingValue, sortMode, nested);
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final float fMissingValue = (Float) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new HalfFloatComparator(numHits, fieldname, null, reversed, enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new HalfFloatLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return HalfFloatValuesComparatorSource.this.getNumericDocValues(context, fMissingValue).getRawFloatValues();
                    }
                };
            }
        };
    }
}
