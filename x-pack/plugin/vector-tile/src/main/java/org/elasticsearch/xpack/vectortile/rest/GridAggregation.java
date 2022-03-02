/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Enum containing the basic operations for different GeoGridAggregations.
 */
enum GridAggregation {
    GEOTILE {
        @Override
        public GeoGridAggregationBuilder newAgg(String aggName) {
            return new GeoTileGridAggregationBuilder(aggName);
        }

        @Override
        public Rectangle bufferTile(Rectangle tile, int z, int gridPrecision) {
            // No buffering needed as GeoTile bins aligns with the tile
            return tile;
        }

        @Override
        public int gridPrecisionToAggPrecision(int z, int gridPrecision) {
            return Math.min(GeoTileUtils.MAX_ZOOM, z + gridPrecision);
        }

        @Override
        public byte[] toGrid(String bucketKey, FeatureFactory featureFactory) throws IOException {
            final Rectangle r = toRectangle(bucketKey);
            return featureFactory.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
        }

        @Override
        public Rectangle toRectangle(String bucketKey) {
            return GeoTileUtils.toBoundingBox(bucketKey);
        }
    },
    GEOHEX {

        // Because hex bins do not fit perfectly on a tile, we need to buffer our queries in order to collect
        // all points inside the bin. We never have aggregations for levels 0 and 1, values for level 2 have
        // been computed manually and approximated. The amount that the buffer decreases by level has been
        // approximated to 2.5 (brute force computation suggest ~2.6 in the first 9 levels so 2.5 should be safe).
        private static final double[] LAT_BUFFER_SIZE = new double[16];
        private static final double[] LON_BUFFER_SIZE = new double[16];
        static {
            LAT_BUFFER_SIZE[0] = LAT_BUFFER_SIZE[1] = Double.NaN;
            LON_BUFFER_SIZE[0] = LON_BUFFER_SIZE[1] = Double.NaN;
            LAT_BUFFER_SIZE[2] = 3.7;
            LON_BUFFER_SIZE[2] = 51.2;
            for (int i = 3; i < LON_BUFFER_SIZE.length; i++) {
                LAT_BUFFER_SIZE[i] = LAT_BUFFER_SIZE[i - 1] / 2.5;
                LON_BUFFER_SIZE[i] = LON_BUFFER_SIZE[i - 1] / 2.5;
            }
        }

        @Override
        public GeoGridAggregationBuilder newAgg(String aggName) {
            return new GeoHexGridAggregationBuilder(aggName);
        }

        @Override
        public Rectangle bufferTile(Rectangle tile, int z, int gridPrecision) {
            if (z == 0 || gridPrecision == 0) {
                // no need to buffer at level 0 as we are looking to all data.
                return tile;
            }
            final int aggPrecision = gridPrecisionToAggPrecision(z, gridPrecision);
            return new Rectangle(
                GeoUtils.normalizeLon(tile.getMinX() - LON_BUFFER_SIZE[aggPrecision]),
                GeoUtils.normalizeLon(tile.getMaxX() + LON_BUFFER_SIZE[aggPrecision]),
                Math.min(GeoTileUtils.LATITUDE_MASK, tile.getMaxY() + LAT_BUFFER_SIZE[aggPrecision]),
                Math.max(-GeoTileUtils.LATITUDE_MASK, tile.getMinY() - LAT_BUFFER_SIZE[aggPrecision])
            );
        }

        @Override
        public int gridPrecisionToAggPrecision(int z, int gridPrecision) {
            // The minimum resolution we allow is 2 to avoid having hexagons containing the pole.
            // this is the table of Hex precision:
            // precision: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
            // -------------------------------------------
            // zoom 0: 2 | 2 | 2 | 2 | 2 | 2 | 3 | 3 |
            // zoom 1: 2 | 2 | 2 | 2 | 2 | 3 | 3 | 4 |
            // zoom 2: 2 | 2 | 2 | 2 | 3 | 3 | 4 | 4 |
            // zoom 3: 2 | 2 | 2 | 3 | 3 | 4 | 4 | 5 |
            // zoom 4: 2 | 2 | 3 | 3 | 4 | 4 | 5 | 5 |
            // zoom 5: 2 | 3 | 3 | 4 | 4 | 5 | 5 | 6 |
            // zoom 6: 3 | 3 | 4 | 4 | 5 | 5 | 6 | 6 |
            // zoom 7: 3 | 4 | 4 | 5 | 5 | 6 | 6 | 7 |
            // zoom 8: 4 | 4 | 5 | 5 | 6 | 6 | 7 | 7 |
            // zoom 9: 4 | 5 | 5 | 6 | 6 | 7 | 7 | 8 |
            // zoom 10: 5 | 5 | 6 | 6 | 7 | 7 | 8 | 8 |
            // zoom 11: 5 | 6 | 6 | 7 | 7 | 8 | 8 | 9 |
            // zoom 12: 6 | 6 | 7 | 7 | 8 | 8 | 9 | 9 |
            // zoom 13: 6 | 7 | 7 | 8 | 8 | 9 | 9 | 10 |
            // zoom 14: 7 | 7 | 8 | 8 | 9 | 9 | 10 | 10 |
            // zoom 15: 7 | 8 | 8 | 9 | 9 | 10 | 10 | 11 |
            // zoom 16: 8 | 8 | 9 | 9 | 10 | 10 | 11 | 11 |
            // zoom 17: 8 | 9 | 9 | 10 | 10 | 11 | 11 | 12 |
            // zoom 18: 9 | 9 | 10 | 10 | 11 | 11 | 12 | 12 |
            // zoom 19: 9 | 10 | 10 | 11 | 11 | 12 | 12 | 13 |
            // zoom 20: 10 | 10 | 11 | 11 | 12 | 12 | 13 | 13 |
            // zoom 21: 10 | 11 | 11 | 12 | 12 | 13 | 13 | 14 |
            // zoom 22: 11 | 11 | 12 | 12 | 13 | 13 | 14 | 14 |
            // zoom 23: 11 | 12 | 12 | 13 | 13 | 14 | 14 | 15 |
            // zoom 24: 12 | 12 | 13 | 13 | 14 | 14 | 15 | 15 |
            // zoom 25: 12 | 13 | 13 | 14 | 14 | 15 | 15 | 15 |
            // zoom 26: 13 | 13 | 14 | 14 | 15 | 15 | 15 | 15 |
            // zoom 27: 13 | 14 | 14 | 15 | 15 | 15 | 15 | 15 |
            // zoom 28: 14 | 14 | 15 | 15 | 15 | 15 | 15 | 15 |
            // zoom 29: 14 | 15 | 15 | 15 | 15 | 15 | 15 | 15 |
            return Math.min(H3.MAX_H3_RES, Math.max(2, (z + gridPrecision - 1) / 2));
        }

        @Override
        public byte[] toGrid(String bucketKey, FeatureFactory featureFactory) {
            final CellBoundary boundary = H3.h3ToGeoBoundary(bucketKey);
            final double[] lats = new double[boundary.numPoints() + 1];
            final double[] lons = new double[boundary.numPoints() + 1];
            for (int i = 0; i < boundary.numPoints(); i++) {
                final LatLng latLng = boundary.getLatLon(i);
                lats[i] = latLng.getLatDeg();
                lons[i] = latLng.getLonDeg();
            }
            lats[boundary.numPoints()] = boundary.getLatLon(0).getLatDeg();
            lons[boundary.numPoints()] = boundary.getLatLon(0).getLonDeg();
            final Polygon polygon = new Polygon(new LinearRing(lons, lats));
            final List<byte[]> x = featureFactory.getFeatures(GeometryNormalizer.apply(Orientation.CCW, polygon));
            return x.size() > 0 ? x.get(0) : null;
        }

        @Override
        public Rectangle toRectangle(String bucketKey) {
            final CellBoundary boundary = H3.h3ToGeoBoundary(bucketKey);
            double minLat = Double.POSITIVE_INFINITY;
            double minLon = Double.POSITIVE_INFINITY;
            double maxLat = Double.NEGATIVE_INFINITY;
            double maxLon = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < boundary.numPoints(); i++) {
                final LatLng latLng = boundary.getLatLon(i);
                minLat = Math.min(minLat, latLng.getLatDeg());
                minLon = Math.min(minLon, latLng.getLonDeg());
                maxLat = Math.max(maxLat, latLng.getLatDeg());
                maxLon = Math.max(maxLon, latLng.getLonDeg());
            }
            return new Rectangle(minLon, maxLon, maxLat, minLat);
        }
    };

    /**
     * New {@link GeoGridAggregationBuilder} instance.
     */
    public abstract GeoGridAggregationBuilder newAgg(String aggName);

    /**
     * Buffer the query bounding box so the bins of an aggregation see
     * all data that is inside them.
     */
    public abstract Rectangle bufferTile(Rectangle tile, int z, int gridPrecision);

    /**
     * Transform the provided grid precision at the given zoom to the
     * agg precision.
     */
    public abstract int gridPrecisionToAggPrecision(int z, int gridPrecision);

    /**
     * transforms the geometry of a given bin into the vector tile feature.
     */
    public abstract byte[] toGrid(String bucketKey, FeatureFactory featureFactory) throws IOException;

    /**
     * Returns the bounding box of the bin.
     */
    public abstract Rectangle toRectangle(String bucketKey);

    public static GridAggregation fromString(String type) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "geotile" -> GEOTILE;
            case "geohex" -> GEOHEX;
            default -> throw new IllegalArgumentException("Invalid agg type [" + type + "]");
        };
    }
}
