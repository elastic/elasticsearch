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
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;

/**
 * Class representing the long-encoded grid-cells belonging to
 * the geo-doc-values. Class must encode the values and then
 * sort them in order to account for the cells correctly.
 */
abstract class CellValues extends AbstractSortingNumericDocValues {
    private MultiGeoPointValues geoValues;
    protected int precision;
    protected CellIdSource.GeoPointLongEncoder encoder;

    protected CellValues(MultiGeoPointValues geoValues, int precision, CellIdSource.GeoPointLongEncoder encoder) {
        this.geoValues = geoValues;
        this.precision = precision;
        this.encoder = encoder;
    }

    @Override
    public boolean advanceExact(int docId) throws IOException {
        if (geoValues.advanceExact(docId)) {
            int docValueCount = geoValues.docValueCount();
            resize(docValueCount);
            int j = 0;
            for (int i = 0; i < docValueCount; i++) {
                j = advanceValue(geoValues.nextValue(), j);
            }
            resize(j);
            sort();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Sets the appropriate long-encoded value for <code>target</code>
     * in <code>values</code>.
     *
     * @param target    the geo-value to encode
     * @param valuesIdx the index into <code>values</code> to set
     * @return          valuesIdx + 1 if value was set, valuesIdx otherwise.
     */
    abstract int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx);
}
