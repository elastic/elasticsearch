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

import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

/** Sorted numeric doc values for geo shapes */
class GeoShapeCellValues extends AbstractSortingNumericDocValues {
    private MultiGeoValues geoValues;
    private int precision;
    private GeoGridTiler tiler;

    protected GeoShapeCellValues(MultiGeoValues geoValues, int precision, GeoGridTiler tiler) {
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
            MultiGeoValues.GeoValue target = geoValues.nextValue();
            // TODO(talevy): determine reasonable circuit-breaker here
            resize(0);
            tiler.setValues(this, target, precision);
            sort();
            return true;
        } else {
            return false;
        }
    }
}
