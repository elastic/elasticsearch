/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link NumericDocValues} instance that wraps a {@link DoubleValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#doubleToSortableLong(double)}.
 */
final class SortableLongBitsNumericDocValues extends LongValues {

    private final DoubleValues values;

    SortableLongBitsNumericDocValues(DoubleValues values) {
        this.values = values;
    }

    @Override
    public long longValue() throws IOException {
        return NumericUtils.doubleToSortableLong(values.doubleValue());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return values.advanceExact(target);
    }

    /** Return the wrapped values. */
    public DoubleValues getDoubleValues() {
        return values;
    }

}
