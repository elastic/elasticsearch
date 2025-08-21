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
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

class HistogramValuesSource extends ValuesSource.Numeric {
    private final Numeric vs;
    private final double interval;

    /**
     *
     * @param vs The original values source
     */
    HistogramValuesSource(Numeric vs, double interval) {
        this.vs = vs;
        this.interval = interval;
    }

    @Override
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        final SortedNumericDoubleValues values = vs.doubleValues(context);
        final NumericDoubleValues singleton = org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(values);
        if (singleton != null) {
            return org.elasticsearch.index.fielddata.FieldData.singleton(doubleSingleValues(singleton));
        } else {
            return doubleMultiValues(values);
        }
    }

    private SortedNumericDoubleValues doubleMultiValues(SortedNumericDoubleValues values) {
        return new SortedNumericDoubleValues() {
            @Override
            public double nextValue() throws IOException {
                return Math.floor(values.nextValue() / interval) * interval;
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

    private NumericDoubleValues doubleSingleValues(NumericDoubleValues values) {
        return new NumericDoubleValues() {
            @Override
            public double doubleValue() throws IOException {
                return Math.floor(values.doubleValue() / interval) * interval;
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
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
