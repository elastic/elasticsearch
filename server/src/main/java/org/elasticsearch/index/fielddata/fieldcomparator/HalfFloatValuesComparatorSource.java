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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.DenseDoubleValues;
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
    public SortField.Type sortType() {
        return SortField.Type.CUSTOM;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final float fMissingValue = (Float) missingObject(missingValue, reversed);
        return new HalfFloatComparator(numHits, fieldname, fMissingValue, reversed, enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new HalfFloatLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return DenseDoubleValues.asNumericDocValues(
                            getDenseDoubleValues(context, fMissingValue),
                            context.reader().maxDoc(),
                            v -> Float.floatToRawIntBits((float) v)
                        );
                    }
                };
            }
        };
    }
}
