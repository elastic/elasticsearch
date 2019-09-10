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

import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

// TODO(talevy): more tests
public class GeoGridTilerTests extends ESTestCase {
    private static final GeoGridTiler.GeoTileGridTiler GEOTILE = GeoGridTiler.GeoTileGridTiler.INSTANCE;
    private static final GeoGridTiler.GeoHashGridTiler GEOHASH = GeoGridTiler.GeoHashGridTiler.INSTANCE;

    public void testGeoTile() {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(GEOTILE.encode(x, y, precision), equalTo(GeoTileUtils.longEncode(x, y, precision)));

        Rectangle tile = GeoTileUtils.toBoundingBox(1309, 3166, 13);
        int count = GEOTILE.getBoundingTileCount(new MultiGeoValues.GeoShapeValue(
            new Extent(
                GeoShapeCoordinateEncoder.INSTANCE.encodeY(tile.getMaxY() - 0.00001),
                GeoShapeCoordinateEncoder.INSTANCE.encodeY(tile.getMinY() + 0.00001),
                GeoShapeCoordinateEncoder.INSTANCE.encodeX(tile.getMinX() + 0.00001),
                GeoShapeCoordinateEncoder.INSTANCE.encodeX(tile.getMaxX() - 0.00001),
                Integer.MAX_VALUE,
                Integer.MIN_VALUE
            )
        ), 13);
        assertThat(count, equalTo(1));
    }

    public void testGeoHash() {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(GEOHASH.encode(x, y, precision), equalTo(Geohash.longEncode(x, y, precision)));
    }
}
