/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
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

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        SortedNumericDocValues values = vs.longValues(context);
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

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("not applicable");
    }
}
