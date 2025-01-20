/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        final SortedNumericDocValues values = vs.longValues(context);
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        return singleton != null ? DocValues.singleton(longSingleValues(singleton)) : longMultiValues(values);
    }

    private SortedNumericDocValues longMultiValues(SortedNumericDocValues values) {
        return new SortedNumericDocValues() {
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

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }

            @Override
            public long cost() {
                return values.cost();
            }
        };
    }

    private NumericDocValues longSingleValues(NumericDocValues values) {
        return new NumericDocValues() {
            @Override
            public long longValue() throws IOException {
                return round(values.longValue());
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }

            @Override
            public long cost() {
                return values.cost();
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
