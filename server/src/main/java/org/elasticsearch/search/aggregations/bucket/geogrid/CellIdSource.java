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
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * Wrapper class to help convert {@link MultiGeoValues}
 * to numeric long values for bucketing.
 */
public class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.Geo valuesSource;
    private final int precision;
    private final GeoGridTiler encoder;

    public CellIdSource(ValuesSource.Geo valuesSource, int precision, GeoGridTiler encoder) {
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
            return new GeoShapeCellValues(geoValues, precision, encoder);
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
}
