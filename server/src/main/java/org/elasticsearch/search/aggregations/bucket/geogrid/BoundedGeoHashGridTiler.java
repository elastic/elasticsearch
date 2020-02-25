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

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoValues;

public class BoundedGeoHashGridTiler extends GeoHashGridTiler {
    private final double boundsTop;
    private final double boundsBottom;
    private final double boundsWestLeft;
    private final double boundsWestRight;
    private final double boundsEastLeft;
    private final double boundsEastRight;
    private final boolean crossesDateline;

    BoundedGeoHashGridTiler(GeoBoundingBox geoBoundingBox) {
        // split geoBoundingBox into west and east boxes
        boundsTop = geoBoundingBox.top();
        boundsBottom = geoBoundingBox.bottom();
        if (geoBoundingBox.right() < geoBoundingBox.left()) {
            boundsWestLeft = -180;
            boundsWestRight = geoBoundingBox.right();
            boundsEastLeft = geoBoundingBox.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else { // only set east bounds
            boundsEastLeft = geoBoundingBox.left();
            boundsEastRight = geoBoundingBox.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }
    }

    @Override
    public int advancePointValue(long[] values, double x, double y, int precision, int valuesIdx) {
        long hash = encode(x, y, precision);
        if (cellIntersectsGeoBoundingBox(Geohash.toBoundingBox(Geohash.stringEncode(hash)))) {
            values[valuesIdx] = hash;
            return valuesIdx + 1;
        }
        return valuesIdx;
    }

    private boolean cellIntersectsGeoBoundingBox(Rectangle rectangle) {
        return (boundsTop >= rectangle.getMinY() && boundsBottom <= rectangle.getMaxY()
            && (boundsEastLeft <= rectangle.getMaxX() && boundsEastRight >= rectangle.getMinX()
            || (crossesDateline && boundsWestLeft <= rectangle.getMaxX() && boundsWestRight >= rectangle.getMinX())));
    }

    protected int setValue(CellValues docValues, MultiGeoValues.GeoValue geoValue, MultiGeoValues.BoundingBox bounds, int precision) {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        GeoRelation relation = relateTile(geoValue, bounds.minX(), bounds.minY(), precision);
        if (relation != GeoRelation.QUERY_DISJOINT) {
            docValues.resizeCell(1);
            docValues.add(0, Geohash.longEncode(hash));
            return 1;
        }
        return 0;
    }

    protected GeoRelation relateTile(MultiGeoValues.GeoValue geoValue, double x, double y, int precision) {
        String hash = Geohash.stringEncode(x, y, precision);
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        if (cellIntersectsGeoBoundingBox(rectangle)) {
            return geoValue.relate(rectangle);
        } else {
            return GeoRelation.QUERY_DISJOINT;
        }
    }

    @Override
    protected int setValuesForFullyContainedTile(String hash, CellValues values,
                                               int valuesIndex, int targetPrecision) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            if (hashes[i].length() == targetPrecision && cellIntersectsGeoBoundingBox(Geohash.toBoundingBox(hashes[i]))) {
                values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
            } else {
                valuesIndex = setValuesForFullyContainedTile(hashes[i], values, valuesIndex, targetPrecision);
            }
        }
        return valuesIndex;
    }
}
