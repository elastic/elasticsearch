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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

/**
 * Wrapper class to help convert {@link MultiGeoValues}
 * to numeric long values for bucketing.
 */
public class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.Geo valuesSource;
    private final int precision;
    private final GeoGridTiler encoder;
    private final CircuitBreaker circuitBreaker;

    public CellIdSource(Geo valuesSource, int precision, GeoGridTiler encoder,  CircuitBreaker circuitBreaker) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.encoder = encoder;
        this.circuitBreaker = circuitBreaker;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        MultiGeoValues geoValues = valuesSource.geoValues(ctx);
        if (precision == 0) {
            // special case, precision 0 is the whole world
            return new AllCellValues(geoValues, encoder);
        }
        ValuesSourceType vs = geoValues.valuesSourceType();
        if (CoreValuesSourceType.GEOPOINT == vs) {
            // docValues are geo points
            return new GeoPointCellValues(geoValues, precision, encoder);
        } else if (CoreValuesSourceType.GEOSHAPE == vs || CoreValuesSourceType.GEO == vs) {
            // docValues are geo shapes
            return new GeoShapeCellValues(geoValues, precision, encoder, circuitBreaker);
        } else {
            throw new IllegalArgumentException("unsupported geo type");
        }
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /** Sorted numeric doc values for geo shapes */
    protected static class GeoShapeCellValues extends BytesTrackingSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoGridTiler tiler;
        private CircuitBreaker circuitBreaker;
        private int maxReservedArrayLength;

        protected GeoShapeCellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler, CircuitBreaker circuitBreaker) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.tiler = tiler;
            this.circuitBreaker = circuitBreaker;
            // account for initialized values array of length 1
            this.maxReservedArrayLength = 1;
            circuitBreaker.addEstimateBytesAndMaybeBreak(Long.BYTES, "geogrid-cell-counter");
        }

        protected void resizeCell(int newSize) {
            int oldValuesLength = values.length;
            resize(newSize);
            int newValuesLength = values.length;
            if (newValuesLength > oldValuesLength && maxReservedArrayLength < newValuesLength) {
                maxReservedArrayLength = newValuesLength;
                long bytesDiff = (newValuesLength - oldValuesLength) * Long.BYTES;
                circuitBreaker.addEstimateBytesAndMaybeBreak(bytesDiff, "geogrid-cell-counter");
            }
        }

        protected void add(int idx, long value) {
           values[idx] = value;
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                ValuesSourceType vs = geoValues.valuesSourceType();
                MultiGeoValues.GeoValue target = geoValues.nextValue();
                resize(0);
                tiler.setValues(this, target, precision);
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    /** Sorted numeric doc values for geo points */
    protected static class GeoPointCellValues extends BytesTrackingSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoGridTiler tiler;

        protected GeoPointCellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.tiler = tiler;
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());
                for (int i = 0; i < docValueCount(); ++i) {
                    MultiGeoValues.GeoValue target = geoValues.nextValue();
                    values[i] = tiler.encode(target.lon(), target.lat(), precision);
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    /** Sorted numeric doc values for precision 0 */
    protected static class AllCellValues extends BytesTrackingSortingNumericDocValues {
        private MultiGeoValues geoValues;

        protected AllCellValues(MultiGeoValues geoValues, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            resize(1);
            values[0] = tiler.encode(0, 0, 0);
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            resize(1);
            return geoValues.advanceExact(docId);
        }
    }

    abstract static class BytesTrackingSortingNumericDocValues extends AbstractSortingNumericDocValues {
        long getValuesBytes() {
            return values.length * Long.BYTES;
        }
    }
}
