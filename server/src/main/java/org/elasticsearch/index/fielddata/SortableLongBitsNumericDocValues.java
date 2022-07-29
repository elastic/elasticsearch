/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link NumericDocValues} instance that wraps a {@link NumericDoubleValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#doubleToSortableLong(double)}.
 */
final class SortableLongBitsNumericDocValues extends AbstractNumericDocValues {

    private int docID = -1;
    private final NumericDoubleValues values;

    SortableLongBitsNumericDocValues(NumericDoubleValues values) {
        this.values = values;
    }

    @Override
    public long longValue() throws IOException {
        return NumericUtils.doubleToSortableLong(values.doubleValue());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        docID = target;
        return values.advanceExact(target);
    }

    @Override
    public int docID() {
        return docID;
    }

    /** Return the wrapped values. */
    public NumericDoubleValues getDoubleValues() {
        return values;
    }

}
