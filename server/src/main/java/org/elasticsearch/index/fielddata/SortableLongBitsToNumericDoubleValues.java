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
 * {@link NumericDoubleValues} instance that wraps a {@link NumericDocValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 */
final class SortableLongBitsToNumericDoubleValues extends NumericDoubleValues {

    private final NumericDocValues values;

    SortableLongBitsToNumericDoubleValues(NumericDocValues values) {
        this.values = values;
    }

    @Override
    public double doubleValue() throws IOException {
        return NumericUtils.sortableLongToDouble(values.longValue());
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
    }

    /** Return the wrapped values. */
    public NumericDocValues getLongValues() {
        return values;
    }

}
