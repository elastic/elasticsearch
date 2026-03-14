/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.LongValues;

import java.io.IOException;

/**
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * that works for single or multi-valued types.
 */
final class SingletonSortedNumericLongValues extends SortedNumericLongValues {
    private final LongValues in;

    SingletonSortedNumericLongValues(LongValues in) {
        this.in = in;
    }

    /** Return the wrapped {@link LongValues} */
    public LongValues getNumericLongValues() {
        return in;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return in.advanceExact(target);
    }

    @Override
    public int docValueCount() {
        return 1;
    }

    @Override
    public long nextValue() throws IOException {
        return in.longValue();
    }

}
