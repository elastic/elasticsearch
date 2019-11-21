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
import org.elasticsearch.geometry.utils.Geohash;
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
     * computes the number of tiles for a specific precision that the geo value's
     * bounding-box is contained within.
     *
     *  @param geoValue  the input shape
     * @param precision the tile zoom-level
     */
    long getBoundingTileCount(MultiGeoValues.GeoValue geoValue, int precision);

    /**
     *
     * @param docValues  the array of long-encoded bucket keys to fill
     * @param geoValue   the input shape
     * @param precision  the tile zoom-level
     *
     * @return the number of tiles the geoValue intersects
     */
    int setValues(long[] docValues, MultiGeoValues.GeoValue geoValue, int precision);

    class GeoHashGridTiler implements GeoGridTiler {
        public static final GeoHashGridTiler INSTANCE = new GeoHashGridTiler();

        private GeoHashGridTiler() {}

        @Override
        public long encode(double x, double y, int precision) {
            return Geohash.longEncode(x, y, precision);
        }

        @Override
        public long getBoundingTileCount(MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            // find minimum (x,y) of geo-hash-cell that contains (bounds.minX, bounds.minY)
            String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            Rectangle geoHashCell = Geohash.toBoundingBox(hash);
            long numLonCells = Math.max(1, (long) Math.ceil(
                (bounds.maxX() - geoHashCell.getMinX()) / Geohash.lonWidthInDegrees(precision)));
            long numLatCells = Math.max(1, (long) Math.ceil(
                (bounds.maxY() - geoHashCell.getMinY()) / Geohash.latHeightInDegrees(precision)));
            return numLonCells * numLatCells;
        }

        @Override
        public int setValues(long[] values, MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            int idx = 0;
            // find minimum (x,y) of geo-hash-cell that contains (bounds.minX, bounds.minY)
            String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            Rectangle geoHashCell = Geohash.toBoundingBox(hash);
            for (double i = geoHashCell.getMinX(); i < bounds.maxX(); i+= Geohash.lonWidthInDegrees(precision)) {
                for (double j = geoHashCell.getMinY(); j < bounds.maxY(); j += Geohash.latHeightInDegrees(precision)) {
                    Rectangle rectangle = Geohash.toBoundingBox(Geohash.stringEncode(i, j, precision));
                    GeoRelation relation = geoValue.relate(rectangle);
                    if (relation != GeoRelation.QUERY_DISJOINT) {
                        values[idx++] = encode(i, j, precision);
                    }
                }
            }

            return idx;
        }
    }

    class GeoTileGridTiler implements GeoGridTiler {
        public static final GeoTileGridTiler INSTANCE = new GeoTileGridTiler();

        private GeoTileGridTiler() {}

        @Override
        public long encode(double x, double y, int precision) {
            return GeoTileUtils.longEncode(x, y, precision);
        }

        @Override
        public long getBoundingTileCount(MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            final double tiles = 1 << precision;
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            return (maxXTile - minXTile + 1) * (maxYTile - minYTile + 1);
        }

        @Override
        public int setValues(long[] values, MultiGeoValues.GeoValue geoValue, int precision) {
            return setValuesByRasterization(0, 0, 0, values, 0, precision, geoValue);
        }

        /**
         *
         * @param values the bucket values as longs
         * @param geoValue the shape value
         * @param precision the target precision to split the shape up into
         * @return the number of buckets the geoValue is found in
         */
        public int setValuesByBruteForceScan(long[] values, MultiGeoValues.GeoValue geoValue, int precision) {
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();

            final double tiles = 1 << precision;
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            int idx = 0;
            for (int i = minXTile; i <= maxXTile; i++) {
                for (int j = minYTile; j <= maxYTile; j++) {
                    Rectangle rectangle = GeoTileUtils.toBoundingBox(i, j, precision);
                    if (geoValue.relate(rectangle) != GeoRelation.QUERY_DISJOINT) {
                        values[idx++] = GeoTileUtils.longEncodeTiles(precision, i, j);
                    }
                }
            }

            return idx;
        }

        private int setValuesByRasterization(int xTile, int yTile, int zTile, long[] values, int valuesIndex, int targetPrecision, MultiGeoValues.GeoValue geoValue) {
            Rectangle rectangle = GeoTileUtils.toBoundingBox(xTile, yTile, zTile);
            GeoRelation relation = geoValue.relate(rectangle);
            if (zTile == targetPrecision) {
                if (GeoRelation.QUERY_DISJOINT != relation) {
                    values[valuesIndex++] = GeoTileUtils.longEncodeTiles(zTile, xTile, yTile);
                }
                return valuesIndex;
            }

            if (GeoRelation.QUERY_INSIDE == relation) {
                return setValuesForFullyContainedTile(xTile, yTile, zTile, values, valuesIndex, targetPrecision);
            }
            if (GeoRelation.QUERY_CROSSES == relation) {
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < 2; j++) {
                        int nextX = 2 * xTile + i;
                        int nextY = 2 * yTile + j;
                        valuesIndex = setValuesByRasterization(nextX, nextY, zTile + 1, values, valuesIndex, targetPrecision, geoValue);
                    }
                }
            }

            return valuesIndex;
        }

        private int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, long[] values, int valuesIndex, int targetPrecision) {
            if (zTile == targetPrecision) {
                values[valuesIndex] = GeoTileUtils.longEncodeTiles(zTile, xTile, yTile);
                return valuesIndex + 1;
            }

            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    int nextX = 2 * xTile + i;
                    int nextY = 2 * yTile + j;
                    valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile + 1, values, valuesIndex, targetPrecision);
                }
            }

            return valuesIndex;
        }
    }
}
