/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;

/**
 * Implements the logic for the Geohash aggregation over a geoshape doc value.
 */
public abstract class GeoHashGridTiler extends GeoGridTiler {

    private GeoHashGridTiler(int precision) {
        super(precision);
    }

    /** Factory method to create GeoHashGridTiler objects */
    public static GeoHashGridTiler makeGridTiler(int precision, GeoBoundingBox geoBoundingBox) {
        return geoBoundingBox == null || geoBoundingBox.isUnbounded()
            ? new UnboundedGeoHashGridTiler(precision)
            : new BoundedGeoHashGridTiler(precision, geoBoundingBox);
    }

    /** check if the provided hash is in the solution space of this tiler */
    protected abstract boolean validHash(String hash);

    @Override
    public long encode(double x, double y) {
        return Geohash.longEncode(x, y, precision);
    }

    @Override
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {

        if (precision == 0) {
            return 1;
        }
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // When the shape represents a point, we compute the hash directly as we do it for GeoPoint
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            return setValue(values, geoValue, bounds);
        }
        // TODO: optimize for when a shape fits in a single tile an
        // for when brute-force is expected to be faster than rasterization, which
        // is when the number of tiles expected is less than the precision
        return setValuesByRasterization("", values, 0, geoValue);
    }

    int setValuesByBruteForceScan(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue, GeoShapeValues.BoundingBox bounds)
        throws IOException {
        // TODO: This way to discover cells inside of a bounding box seems not to work as expected. I can
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
                    values.add(idx++, encode(i, j));
                }
            }
        }
        return idx;
    }

    /**
     * Sets a singular doc-value for the {@link GeoShapeValues.GeoShapeValue}.
     */
    private int setValue(GeoShapeCellValues docValues, GeoShapeValues.GeoShapeValue geoValue, GeoShapeValues.BoundingBox bounds)
        throws IOException {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        if (relateTile(geoValue, hash) != GeoRelation.QUERY_DISJOINT) {
            docValues.resizeCell(1);
            docValues.add(0, Geohash.longEncode(hash));
            return 1;
        }
        return 0;
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, String hash) throws IOException {
        if (validHash(hash)) {
            final Rectangle rectangle = Geohash.toBoundingBox(hash);
            int minX = GeoEncodingUtils.encodeLongitude(rectangle.getMinLon());
            int minY = GeoEncodingUtils.encodeLatitude(rectangle.getMinLat());
            int maxX = GeoEncodingUtils.encodeLongitude(rectangle.getMaxLon());
            int maxY = GeoEncodingUtils.encodeLatitude(rectangle.getMaxLat());
            return geoValue.relate(minX, maxX == Integer.MAX_VALUE ? maxX : maxX - 1, minY, maxY == Integer.MAX_VALUE ? maxY : maxY - 1);
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    /**
     * Recursively search the geohash tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    // pkg protected for testing
    int setValuesByRasterization(String hash, GeoShapeCellValues values, int valuesIndex, GeoShapeValues.GeoShapeValue geoValue)
        throws IOException {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (String s : hashes) {
            GeoRelation relation = relateTile(geoValue, s);
            if (relation == GeoRelation.QUERY_INSIDE) {
                if (s.length() == precision) {
                    values.resizeCell(valuesIndex + 1);
                    values.add(valuesIndex++, Geohash.longEncode(s));
                } else {
                    int numTilesAtPrecision = getNumTilesAtPrecision(precision, hash.length());
                    values.resizeCell(getNewSize(valuesIndex, numTilesAtPrecision + 1));
                    valuesIndex = setValuesForFullyContainedTile(s, values, valuesIndex, precision);
                }
            } else if (relation != GeoRelation.QUERY_DISJOINT) {
                if (s.length() == precision) {
                    values.resizeCell(valuesIndex + 1);
                    values.add(valuesIndex++, Geohash.longEncode(s));
                } else {
                    valuesIndex = setValuesByRasterization(s, values, valuesIndex, geoValue);
                }
            }
        }
        return valuesIndex;
    }

    private int getNewSize(int valuesIndex, int increment) {
        long newSize = (long) valuesIndex + increment;
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) newSize;
    }

    private int getNumTilesAtPrecision(int finalPrecision, int currentPrecision) {
        final long numTilesAtPrecision = Math.min((long) Math.pow(32, finalPrecision - currentPrecision) + 1, getMaxCells());
        if (numTilesAtPrecision > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) numTilesAtPrecision;
    }

    // pkg protected for testing
    int setValuesForFullyContainedTile(String hash, GeoShapeCellValues values, int valuesIndex, int targetPrecision) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (String s : hashes) {
            if (validHash(s)) {
                if (s.length() == targetPrecision) {
                    values.add(valuesIndex++, Geohash.longEncode(s));
                } else {
                    valuesIndex = setValuesForFullyContainedTile(s, values, valuesIndex, targetPrecision);
                }
            }
        }
        return valuesIndex;
    }

    /**
     * Bounded geotile aggregation. It accepts hashes that intersects the provided bounds.
     */
    private static class BoundedGeoHashGridTiler extends GeoHashGridTiler {
        private final GeoHashBoundedPredicate predicate;

        BoundedGeoHashGridTiler(int precision, GeoBoundingBox bbox) {
            super(precision);
            this.predicate = new GeoHashBoundedPredicate(precision, bbox);
        }

        @Override
        protected long getMaxCells() {
            return predicate.getMaxHashes();
        }

        @Override
        protected boolean validHash(String hash) {
            return predicate.validHash(hash);
        }
    }

    /**
     * Unbounded geohash aggregation. It accepts any hash.
     */
    private static class UnboundedGeoHashGridTiler extends GeoHashGridTiler {

        private final long maxHashes;

        UnboundedGeoHashGridTiler(int precision) {
            super(precision);
            this.maxHashes = (long) Math.pow(32, precision);
        }

        @Override
        protected boolean validHash(String hash) {
            return true;
        }

        @Override
        protected long getMaxCells() {
            return maxHashes;
        }
    }
}
