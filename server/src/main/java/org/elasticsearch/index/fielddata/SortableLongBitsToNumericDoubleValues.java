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
 * {@link DoubleValues} instance that wraps a {@link NumericDocValues}
 * and converts the doubles to sortable long bits using
 * {@link NumericUtils#sortableLongToDouble(long)}.
 */
final class SortableLongBitsToNumericDoubleValues extends DoubleValues {

    private final LongValues values;

    SortableLongBitsToNumericDoubleValues(LongValues values) {
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
    public LongValues getLongValues() {
        return values;
    }

}
