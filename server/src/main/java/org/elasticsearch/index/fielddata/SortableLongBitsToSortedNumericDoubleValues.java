/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link SortedNumericDoubleValues} instance that wraps a {@link SortedNumericDocValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 */
final class SortableLongBitsToSortedNumericDoubleValues extends SortedNumericDoubleValues {

    private final SortedNumericDocValues values;

    SortableLongBitsToSortedNumericDoubleValues(SortedNumericDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    @Override
    public double nextValue() throws IOException {
        return NumericUtils.sortableLongToDouble(values.nextValue());
    }

    @Override
    public int docValueCount() {
        return values.docValueCount();
    }

    /** Return the wrapped values. */
    public SortedNumericDocValues getLongValues() {
        return values;
    }

}
