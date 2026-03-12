/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LongValues;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

/**
 * A wrapper for {@link ValuesSource.Numeric} that uses {@link Rounding} to transform the long values
 * produced by the underlying source.
 */
class RoundingValuesSource extends ValuesSource.Numeric {
    private final ValuesSource.Numeric vs;
    private final Rounding.Prepared rounding;

    /**
     *
     * @param vs The original values source
     * @param rounding How to round the values
     */
    RoundingValuesSource(Numeric vs, Rounding.Prepared rounding) {
        this.vs = vs;
        this.rounding = rounding;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    public long round(long value) {
        return rounding.round(value);
    }

    public double roundingSize(long milliSeconds, Rounding.DateTimeUnit unit) {
        return rounding.roundingSize(milliSeconds, unit);
    }

    public double roundingSize(Rounding.DateTimeUnit unit) {
        return rounding.roundingSize(unit);
    }

    @Override
    public SortedNumericLongValues longValues(LeafReaderContext context) throws IOException {
        final SortedNumericLongValues values = vs.longValues(context);
        final LongValues singleton = SortedNumericLongValues.unwrapSingleton(values);
        return singleton != null ? SortedNumericLongValues.singleton(longSingleValues(singleton)) : longMultiValues(values);
    }

    private SortedNumericLongValues longMultiValues(SortedNumericLongValues values) {
        return new SortedNumericLongValues() {
            @Override
            public long nextValue() throws IOException {
                return round(values.nextValue());
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }
        };
    }

    private LongValues longSingleValues(LongValues values) {
        return new LongValues() {
            @Override
            public long longValue() throws IOException {
                return round(values.longValue());
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }
        };
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
