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
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.GeoPoint valuesSource;
    private final int precision;

    CellIdSource(GeoPoint valuesSource, int precision) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different geohash cells.
        this.precision = precision;
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
        return new CellValues(valuesSource.geoPointValues(ctx), precision);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    private static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoPointValues geoValues;
        private int precision;

        protected CellValues(MultiGeoPointValues geoValues, int precision) {
            this.geoValues = geoValues;
            this.precision = precision;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());
                for (int i = 0; i < docValueCount(); ++i) {
                    org.elasticsearch.common.geo.GeoPoint target = geoValues.nextValue();
                    values[i] = GeoHashUtils.longEncode(target.getLon(), target.getLat(),
                        precision);
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }
}
