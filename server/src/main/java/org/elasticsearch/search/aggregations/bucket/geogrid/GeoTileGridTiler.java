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

import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fielddata.MultiGeoValues;

public class GeoTileGridTiler implements GeoGridTiler {

    @Override
    public long encode(double x, double y, int precision) {
        return GeoTileUtils.longEncode(x, y, precision);
    }

    public int advancePointValue(long[] values, double x, double y, int precision, int valuesIdx) {
        values[valuesIdx] = encode(x, y, precision);
        return valuesIdx + 1;
    }

    /**
     * Sets the values of the long[] underlying {@link CellValues}.
     *
     * If the shape resides between <code>GeoTileUtils.NORMALIZED_LATITUDE_MASK</code> and 90 or
     * between <code>GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK</code> and -90 degree latitudes, then
     * the shape is not accounted for since geo-tiles are only defined within those bounds.
     *
     * @param values           the bucket values
     * @param geoValue         the input shape
     * @param precision        the tile zoom-level
     *
     * @return the number of tiles set by the shape
     */
    @Override
    public int setValues(CellValues values, MultiGeoValues.GeoValue geoValue, int precision) {
        MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        if (precision == 0) {
            values.resizeCell(1);
            values.add(0, GeoTileUtils.longEncodeTiles(0, 0, 0));
            return 1;
        }

        // geo tiles are not defined at the extreme latitudes due to them
        // tiling the world as a square.
        if ((bounds.top > GeoTileUtils.NORMALIZED_LATITUDE_MASK && bounds.bottom > GeoTileUtils.NORMALIZED_LATITUDE_MASK)
                || (bounds.top < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK
                && bounds.bottom < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK)) {
            return 0;
        }

        final double tiles = 1 << precision;
        int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
        int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
        int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
        int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
        int count = (maxXTile - minXTile + 1) * (maxYTile - minYTile + 1);
        if (count == 1) {
            return setValue(values, geoValue, minXTile, minYTile, precision);
        } else if (count <= precision) {
            return setValuesByBruteForceScan(values, geoValue, precision, minXTile, minYTile, maxXTile, maxYTile);
        } else {
            return setValuesByRasterization(0, 0, 0, values, 0, precision, geoValue);
        }
    }

    protected GeoRelation relateTile(MultiGeoValues.GeoValue geoValue, int xTile, int yTile, int precision) {
        Rectangle rectangle = GeoTileUtils.toBoundingBox(xTile, yTile, precision);
        return geoValue.relate(rectangle);
    }

    /**
     * Sets a singular doc-value for the {@link MultiGeoValues.GeoValue}. To be overriden by {@link BoundedGeoTileGridTiler}
     * to account for {@link org.elasticsearch.common.geo.GeoBoundingBox} conditions
     */
    protected int setValue(CellValues docValues, MultiGeoValues.GeoValue geoValue, int xTile, int yTile, int precision) {
        docValues.resizeCell(1);
        docValues.add(0, GeoTileUtils.longEncodeTiles(precision, xTile, yTile));
        return 1;
    }

    /**
     *
     * @param values the bucket values as longs
     * @param geoValue the shape value
     * @param precision the target precision to split the shape up into
     * @return the number of buckets the geoValue is found in
     */
    protected int setValuesByBruteForceScan(CellValues values, MultiGeoValues.GeoValue geoValue,
                                            int precision, int minXTile, int minYTile, int maxXTile, int maxYTile) {
        int idx = 0;
        for (int i = minXTile; i <= maxXTile; i++) {
            for (int j = minYTile; j <= maxYTile; j++) {
                GeoRelation relation = relateTile(geoValue, i, j, precision);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    values.resizeCell(idx + 1);
                    values.add(idx++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        }
        return idx;
    }

    protected int setValuesByRasterization(int xTile, int yTile, int zTile, CellValues values, int valuesIndex,
                                           int targetPrecision, MultiGeoValues.GeoValue geoValue) {
        zTile++;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                int nextX = 2 * xTile + i;
                int nextY = 2 * yTile + j;
                GeoRelation relation = relateTile(geoValue, nextX, nextY, zTile);
                if (GeoRelation.QUERY_INSIDE == relation) {
                    if (zTile == targetPrecision) {
                        values.resizeCell(valuesIndex + 1);
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        values.resizeCell(valuesIndex +  1 << ( 2 * (targetPrecision - zTile)) + 1);
                        valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile, values, valuesIndex, targetPrecision);
                    }
                } else if (GeoRelation.QUERY_CROSSES == relation) {
                    if (zTile == targetPrecision) {
                        values.resizeCell(valuesIndex + 1);
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        valuesIndex = setValuesByRasterization(nextX, nextY, zTile, values, valuesIndex, targetPrecision, geoValue);
                    }
                }
            }
        }
        return valuesIndex;
    }

    protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, CellValues values, int valuesIndex,
                                               int targetPrecision) {
        zTile++;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                int nextX = 2 * xTile + i;
                int nextY = 2 * yTile + j;
                if (zTile == targetPrecision) {
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                } else {
                    valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile, values, valuesIndex, targetPrecision);
                }
            }
        }
        return valuesIndex;
    }
}
