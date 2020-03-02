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

import org.elasticsearch.index.fielddata.MultiGeoValues;

/**
 * The tiler to use to convert a geo value into long-encoded bucket keys for aggregating.
 */
public interface GeoGridTiler {
    /**
     * encodes a single point to its long-encoded bucket key value.
     *
     * @param x        the x-coordinate
     * @param y        the y-coordinate
     * @param precision  the zoom level of tiles
     */
    long encode(double x, double y, int precision);

    /**
     *
     * @param docValues        the array of long-encoded bucket keys to fill
     * @param geoValue         the input shape
     * @param precision        the tile zoom-level
     *
     * @return the number of tiles the geoValue intersects
     */
    int setValues(CellValues docValues, MultiGeoValues.GeoValue geoValue, int precision);


    /**
     * This sets the long-encoded value of the geo-point into the associated doc-values
     * array. This is to be overridden by the {@link BoundedGeoTileGridTiler} and
     * {@link BoundedGeoHashGridTiler} to check whether the point's tile intersects
     * the appropriate bounds.
     *
     * @param values      the doc-values array
     * @param x           the longitude of the point
     * @param y           the latitude of the point
     * @param precision   the zoom-level
     * @param valuesIdx   the index into the doc-values array at the time of advancement
     *
     * @return the next index into the array
     */
    default int advancePointValue(long[] values, double x, double y, int precision, int valuesIdx) {
        values[valuesIdx] = encode(x, y, precision);
        return valuesIdx + 1;
    }
}
