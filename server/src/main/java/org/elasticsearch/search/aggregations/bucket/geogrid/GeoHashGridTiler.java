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

public class GeoHashGridTiler implements GeoGridTiler {

    @Override
    public long encode(double x, double y, int precision) {
        return Geohash.longEncode(x, y, precision);
    }

    @Override
    public int setValues(CellValues values, MultiGeoValues.GeoValue geoValue, int precision) {
        if (precision == 1) {
            values.resizeCell(1);
            values.add(0, Geohash.longEncode(0, 0, 0));
        }

        MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();
        long numLonCells = (long) ((bounds.maxX() - bounds.minX()) / Geohash.lonWidthInDegrees(precision));
        long numLatCells = (long) ((bounds.maxY() - bounds.minY()) / Geohash.latHeightInDegrees(precision));
        long count = (numLonCells + 1) * (numLatCells + 1);
        if (count == 1) {
            return setValue(values, geoValue, bounds, precision);
        } else if (count <= precision) {
            return setValuesByBruteForceScan(values, geoValue, precision, bounds);
        } else {
            return setValuesByRasterization("", values, 0, precision, geoValue);
        }
    }

    /**
     * Sets a singular doc-value for the {@link MultiGeoValues.GeoValue}. To be overriden by {@link BoundedGeoHashGridTiler}
     * to account for {@link org.elasticsearch.common.geo.GeoBoundingBox} conditions
     */
    protected int setValue(CellValues docValues, MultiGeoValues.GeoValue geoValue, MultiGeoValues.BoundingBox bounds, int precision) {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        docValues.resizeCell(1);
        docValues.add(0, Geohash.longEncode(hash));
        return 1;
    }

    protected GeoRelation relateTile(MultiGeoValues.GeoValue geoValue, String hash) {
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        return geoValue.relate(rectangle);
    }

    protected int setValuesByBruteForceScan(CellValues values, MultiGeoValues.GeoValue geoValue, int precision,
                                            MultiGeoValues.BoundingBox bounds) {
        // TODO: This way to discover cells inside of a bounding box seems not to work as expected. I  can
        // see that eventually we will be visiting twice the same cell which should not happen.
        int idx = 0;
        String min = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        String max = Geohash.stringEncode(bounds.maxX(), bounds.maxY(), precision);
        String minNeighborBelow = Geohash.getNeighbor(min, precision, 0, -1);
        double minY = Geohash.decodeLatitude((minNeighborBelow == null) ? min : minNeighborBelow);
        double minX = Geohash.decodeLongitude(min);
        double maxY = Geohash.decodeLatitude(max);
        double maxX = Geohash.decodeLongitude(max);
        for (double i = minX; i <= maxX; i += Geohash.lonWidthInDegrees(precision)) {
            for (double j = minY; j <= maxY; j += Geohash.latHeightInDegrees(precision)) {
                String hash = Geohash.stringEncode(i, j, precision);
                GeoRelation relation = relateTile(geoValue, hash);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    values.resizeCell(idx + 1);
                    values.add(idx++,  encode(i, j, precision));
                }
            }
        }
        return idx;
    }

    protected int setValuesByRasterization(String hash, CellValues values, int valuesIndex, int targetPrecision,
                                           MultiGeoValues.GeoValue geoValue) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            GeoRelation relation = relateTile(geoValue, hashes[i]);
            if (relation == GeoRelation.QUERY_CROSSES) {
                if (hashes[i].length() == targetPrecision) {
                    values.resizeCell(valuesIndex + 1);
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                } else {
                    valuesIndex =
                        setValuesByRasterization(hashes[i], values, valuesIndex, targetPrecision, geoValue);
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

    protected int setValuesForFullyContainedTile(String hash, CellValues values,
                                               int valuesIndex, int targetPrecision) {
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
