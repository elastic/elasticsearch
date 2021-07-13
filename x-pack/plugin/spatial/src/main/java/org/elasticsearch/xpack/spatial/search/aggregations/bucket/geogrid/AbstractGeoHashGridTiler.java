/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

/**
 * Implements most of the logic for the GeoHash aggregation.
 */
abstract class AbstractGeoHashGridTiler extends GeoGridTiler {

    AbstractGeoHashGridTiler(int precision) {
        super(precision);
    }

    /** check if the provided hash is in the solution space of this tiler */
    protected abstract boolean validHash(String hash);

    @Override
    public long encode(double x, double y) {
        return Geohash.longEncode(x, y, precision);
    }

    @Override
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) {

        if (precision == 0) {
          return 1;
        }
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // When the shape represents a point, we compute the hash directly as we do it for GeoPoint
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            return setValue(values, geoValue, bounds);
        }
        // TODO: optimize for when a  shape fits in a single tile an
        //  for when brute-force is expected to be faster than rasterization, which
        //  is when the number of tiles expected is less than the precision
        return setValuesByRasterization("", values, 0, geoValue);
    }

    protected int setValuesByBruteForceScan(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue,
                                            GeoShapeValues.BoundingBox bounds) {
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
                    values.add(idx++,  encode(i, j));
                }
            }
        }
        return idx;
    }

    /**
     * Sets a singular doc-value for the {@link GeoShapeValues.GeoShapeValue}.
     */
    protected int setValue(GeoShapeCellValues docValues, GeoShapeValues.GeoShapeValue geoValue, GeoShapeValues.BoundingBox bounds) {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        if (relateTile(geoValue, hash) != GeoRelation.QUERY_DISJOINT) {
            docValues.resizeCell(1);
            docValues.add(0, Geohash.longEncode(hash));
            return 1;
        }
        return 0;
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, String hash) {
        return validHash(hash) ? geoValue.relate(Geohash.toBoundingBox(hash)) : GeoRelation.QUERY_DISJOINT;
    }

    protected int setValuesByRasterization(String hash, GeoShapeCellValues values, int valuesIndex,
                                           GeoShapeValues.GeoShapeValue geoValue) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            GeoRelation relation = relateTile(geoValue, hashes[i]);
            if (relation == GeoRelation.QUERY_CROSSES) {
                if (hashes[i].length() == precision) {
                    values.resizeCell(valuesIndex + 1);
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                } else {
                    valuesIndex =
                        setValuesByRasterization(hashes[i], values, valuesIndex, geoValue);
                }
            } else if (relation == GeoRelation.QUERY_INSIDE) {
                if (hashes[i].length() == precision) {
                    values.resizeCell(valuesIndex + 1);
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                } else {
                    int numTilesAtPrecision = getNumTilesAtPrecision(precision, hash.length());
                    values.resizeCell(getNewSize(valuesIndex, numTilesAtPrecision + 1));
                    valuesIndex = setValuesForFullyContainedTile(hashes[i],values, valuesIndex, precision);
                }
            }
        }
        return valuesIndex;
    }

    private int getNewSize(int valuesIndex, int increment) {
        long newSize  = (long) valuesIndex + increment;
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) newSize;
    }

    private int getNumTilesAtPrecision(int finalPrecision, int currentPrecision) {
        final long numTilesAtPrecision  = Math.min((long) Math.pow(32, finalPrecision - currentPrecision) + 1, getMaxCells());
        if (numTilesAtPrecision > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) numTilesAtPrecision;
    }

    protected int setValuesForFullyContainedTile(String hash, GeoShapeCellValues values,
                                                 int valuesIndex, int targetPrecision) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            if (validHash(hashes[i])) {
                if (hashes[i].length() == targetPrecision) {
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                } else {
                    valuesIndex = setValuesForFullyContainedTile(hashes[i], values, valuesIndex, targetPrecision);
                }
            }
        }
        return valuesIndex;
    }
}
