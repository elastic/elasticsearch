/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;

public class GeoHashGridTiler implements GeoGridTiler {

    @Override
    public long encode(double x, double y, int precision) {
        return Geohash.longEncode(x, y, precision);
    }

    @Override
    public int setValues(GeoShapeCellValues values, MultiGeoShapeValues.GeoShapeValue geoValue, int precision) {
        if (precision == 1) {
            values.resizeCell(1);
            values.add(0, Geohash.longEncode(0, 0, 0));
        }

        MultiGeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // TODO: optimize for when a whole shape (not just point) fits in a single tile an
        //  for when brute-force is expected to be faster than rasterization, which
        //  is when the number of tiles expected is less than the precision

        // optimization for setting just one value for when the shape represents a point
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            return setValue(values, geoValue, bounds, precision);
        }
        return setValuesByRasterization("", values, 0, precision, geoValue);
    }

    /**
     * Sets a singular doc-value for the {@link MultiGeoShapeValues.GeoShapeValue}. To be overriden by {@link BoundedGeoHashGridTiler}
     * to account for {@link org.elasticsearch.common.geo.GeoBoundingBox} conditions
     */
    protected int setValue(GeoShapeCellValues docValues, MultiGeoShapeValues.GeoShapeValue geoValue, MultiGeoShapeValues.BoundingBox bounds,
                           int precision) {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        docValues.resizeCell(1);
        docValues.add(0, Geohash.longEncode(hash));
        return 1;
    }

    protected GeoRelation relateTile(MultiGeoShapeValues.GeoShapeValue geoValue, String hash) {
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        return geoValue.relate(rectangle);
    }

    protected int setValuesByBruteForceScan(GeoShapeCellValues values, MultiGeoShapeValues.GeoShapeValue geoValue, int precision,
                                            MultiGeoShapeValues.BoundingBox bounds) {
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

    protected int setValuesByRasterization(String hash, GeoShapeCellValues values, int valuesIndex, int targetPrecision,
                                           MultiGeoShapeValues.GeoShapeValue geoValue) {
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

    protected int setValuesForFullyContainedTile(String hash, GeoShapeCellValues values,
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
