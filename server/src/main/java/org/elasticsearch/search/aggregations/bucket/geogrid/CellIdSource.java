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

    public CellIdSource(Geo valuesSource, int precision, GeoGridTiler encoder) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.encoder = encoder;
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
        return new CellValues(valuesSource.geoValues(ctx), precision, encoder);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    protected static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoGridTiler tiler;

        protected CellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.tiler = tiler;
        }

        protected void resizeCell(int newSize) {
            resize(newSize);
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
                if (CoreValuesSourceType.GEOPOINT == vs) {
                    resize(geoValues.docValueCount());
                    for (int i = 0; i < docValueCount(); ++i) {
                        MultiGeoValues.GeoValue target = geoValues.nextValue();
                        values[i] = tiler.encode(target.lon(), target.lat(), precision);
                    }
                } else if (CoreValuesSourceType.GEOSHAPE == vs || CoreValuesSourceType.GEO == vs) {
                    MultiGeoValues.GeoValue target = geoValues.nextValue();
                    // TODO(talevy): determine reasonable circuit-breaker here
                    resize(0);
                    tiler.setValues(this, target, precision);
                } else {
                    throw new IllegalArgumentException("unsupported geo type");
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }
}
