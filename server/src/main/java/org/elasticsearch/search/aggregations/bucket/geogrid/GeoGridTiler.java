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
     *
     * @param docValues  the array of long-encoded bucket keys to fill
     * @param geoValue   the input shape
     * @param precision  the tile zoom-level
     *
     * @return the number of tiles the geoValue intersects
     */
    int setValues(CellIdSource.CellValues docValues, MultiGeoValues.GeoValue geoValue, int precision);

    class GeoHashGridTiler implements GeoGridTiler {
        public static final GeoHashGridTiler INSTANCE = new GeoHashGridTiler();

        private GeoHashGridTiler() {}

        @Override
        public long encode(double x, double y, int precision) {
            return Geohash.longEncode(x, y, precision);
        }

        @Override
        public int setValues(CellIdSource.CellValues values, MultiGeoValues.GeoValue geoValue, int precision) {
            if (precision == 0) {
                values.resizeCell(1);
                values.add(0, Geohash.longEncode(""));
                return 1;
            }
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            assert bounds.minX() <= bounds.maxX();
            long numLonCells = (long) ((bounds.maxX() - bounds.minX()) / Geohash.lonWidthInDegrees(precision));
            long numLatCells = (long) ((bounds.maxY() - bounds.minY()) / Geohash.latHeightInDegrees(precision));
            long count = (numLonCells + 1) * (numLatCells + 1);
            if (count == 1) {
                String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
                values.resizeCell(1);
                values.add(0, Geohash.longEncode(hash));
                return 1;
            } else if (count <= precision) {
                return setValuesByBruteForceScan(values, geoValue, precision, bounds);
            } else {
                return setValuesByRasterization("", values, 0, precision, geoValue);
            }
        }

        protected int setValuesByBruteForceScan(CellIdSource.CellValues values, MultiGeoValues.GeoValue geoValue,
                                                int precision, MultiGeoValues.BoundingBox bounds) {
            int idx = 0;
            String min = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            String max = Geohash.stringEncode(bounds.maxX(), bounds.maxY(), precision);
            double minY = Geohash.decodeLatitude(min);
            double minX = Geohash.decodeLongitude(min);
            double maxY = Geohash.decodeLatitude(max);
            double maxX = Geohash.decodeLongitude(max);
            for (double i = minX; i <= maxX; i += Geohash.lonWidthInDegrees(precision)) {
                for (double j = minY; j <= maxY; j += Geohash.latHeightInDegrees(precision)) {
                    Rectangle rectangle = Geohash.toBoundingBox(Geohash.stringEncode(i, j, precision));
                    GeoRelation relation = geoValue.relate(rectangle);
                    if (relation != GeoRelation.QUERY_DISJOINT) {
                        values.resizeCell(idx + 1);
                        values.add(idx++,  encode(i, j, precision));
                    }
                }
            }
            return idx;
        }

        protected int setValuesByRasterization(String hash, CellIdSource.CellValues values, int valuesIndex,
                                               int targetPrecision, MultiGeoValues.GeoValue geoValue) {
            String[] hashes = Geohash.getSubGeohashes(hash);
            for (int i = 0; i < hashes.length; i++) {
                Rectangle rectangle = Geohash.toBoundingBox(hashes[i]);
                MultiGeoValues.BoundingBox shapeBounds = geoValue.boundingBox();
                if (shapeBounds.minX() == rectangle.getMaxX() ||
                    shapeBounds.maxY() == rectangle.getMinY()) {
                    continue;
                }
                GeoRelation relation = geoValue.relate(rectangle);
                if (relation == GeoRelation.QUERY_CROSSES) {
                    if (hashes[i].length() == targetPrecision) {
                        values.resizeCell(valuesIndex + 1);
                        values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                    } else {
                        valuesIndex = setValuesByRasterization(hashes[i], values, valuesIndex, targetPrecision, geoValue);
                    }
                } else if (relation == GeoRelation.QUERY_INSIDE) {
                    if (hashes[i].length() == targetPrecision) {
                        values.resizeCell(valuesIndex + 1);
                        values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                    } else {
                        values.resizeCell(valuesIndex + (int) Math.pow(32, targetPrecision - hash.length()) + 1);
                        valuesIndex = setValuesForFullyContainedTile(hashes[i],values, valuesIndex, targetPrecision);
                    }
                }
            }
            return valuesIndex;
        }

        private int setValuesForFullyContainedTile(String hash, CellIdSource.CellValues values, int valuesIndex, int targetPrecision) {
            String[] hashes = Geohash.getSubGeohashes(hash);
            for (int i = 0; i < hashes.length; i++) {
                if (hashes[i].length() == targetPrecision) {
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                } else {
                    valuesIndex = setValuesForFullyContainedTile(hashes[i], values, valuesIndex, targetPrecision);
                }
            }
            return valuesIndex;
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
        public int setValues(CellIdSource.CellValues values, MultiGeoValues.GeoValue geoValue, int precision) {
            if (precision == 0) {
                values.resizeCell(1);
                values.add(0, encode(0, 0, 0));
                return 1;
            }
            MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
            assert bounds.minX() <= bounds.maxX();
            final double tiles = 1 << precision;
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            int count = (maxXTile - minXTile + 1) * (maxYTile - minYTile + 1);
            if (count == 1) {
                values.resizeCell(1);
                values.add(0, GeoTileUtils.longEncodeTiles(precision, minXTile, minYTile));
                return 1;
            } else if (count <= precision) {
                return setValuesByBruteForceScan(values, geoValue, precision, minXTile, minYTile, maxXTile, maxYTile);
            } else {
                return setValuesByRasterization(0, 0, 0, values, 0, precision, geoValue);
            }
        }

        /**
         *
         * @param values the bucket values as longs
         * @param geoValue the shape value
         * @param precision the target precision to split the shape up into
         * @return the number of buckets the geoValue is found in
         */
        protected int setValuesByBruteForceScan(CellIdSource.CellValues values, MultiGeoValues.GeoValue geoValue, int precision,
                                             int minXTile, int minYTile, int maxXTile, int maxYTile) {
            int idx = 0;
            for (int i = minXTile; i <= maxXTile; i++) {
                for (int j = minYTile; j <= maxYTile; j++) {
                    Rectangle rectangle = GeoTileUtils.toBoundingBox(i, j, precision);
                    if (geoValue.relate(rectangle) != GeoRelation.QUERY_DISJOINT) {
                        values.resizeCell(idx + 1);
                        values.add(idx++, GeoTileUtils.longEncodeTiles(precision, i, j));
                    }
                }
            }
            return idx;
        }

        protected int setValuesByRasterization(int xTile, int yTile, int zTile, CellIdSource.CellValues values, int valuesIndex,
                                               int targetPrecision, MultiGeoValues.GeoValue geoValue) {
            zTile++;
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 2; j++) {
                    int nextX = 2 * xTile + i;
                    int nextY = 2 * yTile + j;
                    Rectangle rectangle = GeoTileUtils.toBoundingBox(nextX, nextY, zTile);
                    // TODO: this looks hacky, maybe the relate method should handle it?
                    MultiGeoValues.BoundingBox shapeBounds = geoValue.boundingBox();
                    if (shapeBounds.minX() == rectangle.getMaxX() ||
                        shapeBounds.maxY() == rectangle.getMinY()) {
                        continue;
                    }
                    GeoRelation relation = geoValue.relate(rectangle);
                    if (GeoRelation.QUERY_INSIDE == relation) {
                        if (zTile == targetPrecision) {
                            values.resizeCell(valuesIndex + 1);
                            values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                        } else {
                            values.resizeCell(valuesIndex + (int) Math.pow(4, targetPrecision - zTile) + 1);
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

        private int setValuesForFullyContainedTile(int xTile, int yTile, int zTile,
                                                   CellIdSource.CellValues values, int valuesIndex, int targetPrecision) {
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
}
