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
package org.elasticsearch.common.geo;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashTypeProvider;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;

/**
 * Implements quad key hashing, same as used by map tiles.
 * The string key is formatted as  "zoom/x/y"
 * The hash value (long) contains all three of those values.
 */
public class QuadKeyHash implements GeoHashTypeProvider {

    /**
     * Largest number of tiles (precision) to use.
     * This value cannot be more than (64-5)/2 = 29, because 5 bits are used for zoom level itself
     * If zoom is not stored inside hash, it would be possible to use up to 32.
     * Another consideration is that index optimizes lat/lng storage, loosing some precision.
     * E.g. hash lng=140.74779717298918D lat=45.61884022447444D == "18/233561/93659", but shown as "18/233561/93658"
     */
    public static final int MAX_ZOOM = 29;

    /**
     * Bit position of the zoom value within hash.  Must be >= 2*MAX_ZOOM
     * Keeping it at a constant place allows MAX_ZOOM to be increased
     * without breaking serialization binary compatibility
     * (still, the newer version should not use higher MAX_ZOOM in the mixed cases)
     */
    private static final int ZOOM_SHIFT = 29 * 2;

    /**
     * Mask of all the bits used by the quadkey in a hash
     */
    private static final long QUADKEY_MASK = (1L << ZOOM_SHIFT) - 1;

    private static int validatePrecisionInt(int precision) {
        if (precision < 0 || precision > MAX_ZOOM) {
            throw new IllegalArgumentException("Invalid geohash quadkey aggregation precision of " +
                precision + ". Must be between 0 and " + MAX_ZOOM + ".");
        }
        return precision;
    }

    private static int[] parseHash(final long hash) {
        final int zoom = validatePrecisionInt((int) (hash >>> ZOOM_SHIFT));
        final int tiles = 1 << zoom;

        // decode the quadkey bits as interleaved xtile and ytile
        long val = hash & QUADKEY_MASK;
        int xtile = (int) BitUtil.deinterleave(val);
        int ytile = (int) BitUtil.deinterleave(val >>> 1);
        if (xtile < 0 || ytile < 0 || xtile >= tiles || ytile >= tiles) {
            throw new IllegalArgumentException("hash-tile");
        }

        return new int[]{zoom, xtile, ytile};
    }

    private static double tile2lon(final double x, final double tiles) {
        return x / tiles * 360.0 - 180;
    }

    private static double tile2lat(final double y, final double tiles) {
        double n = Math.PI - (2.0 * Math.PI * y) / tiles;
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    @Override
    public int getDefaultPrecision() {
        return 5;
    }

    @Override
    public int parsePrecisionString(String precision) {
        try {
            // we want to treat simple integer strings as precision levels, not distances
            return validatePrecision(Integer.parseInt(precision));
            // Do not catch IllegalArgumentException here
        } catch (NumberFormatException e) {
            // try to parse as a distance value
            final int parsedPrecision = GeoUtils.quadTreeLevelsForPrecision(precision);
            try {
                return validatePrecision(parsedPrecision);
            } catch (IllegalArgumentException e2) {
                // this happens when distance too small, so precision > .
                // We'd like to see the original string
                throw new IllegalArgumentException("precision too high [" + precision + "]", e2);
            }
        }
    }

    @Override
    public int validatePrecision(int precision) {
        return validatePrecisionInt(precision);
    }

    @Override
    public long calculateHash(double longitude, double latitude, int precision) {
        // Adapted from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

        // How many tiles in X and in Y
        final int tiles = 1 << validatePrecision(precision);
        final double lon = normalizeLon(longitude);
        final double lat = normalizeLat(latitude);

        int xtile = (int) Math.floor((lon + 180) / 360 * tiles);
        int ytile = (int) Math.floor(
            (1 - Math.log(
                Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))
            ) / Math.PI) / 2 * tiles);
        if (xtile < 0)
            xtile = 0;
        if (xtile >= tiles)
            xtile = (tiles - 1);
        if (ytile < 0)
            ytile = 0;
        if (ytile >= tiles)
            ytile = (tiles - 1);

        // Zoom value is placed in front of all the bits used for the quadkey
        // e.g. if max zoom is 26, the largest index would use 52 bits (51st..0th),
        // leaving 12 bits unused for zoom. See MAX_ZOOM comment above.
        return BitUtil.interleave(xtile, ytile) | ((long) precision << ZOOM_SHIFT);
    }

    @Override
    public String hashAsString(long hash) {
        int[] res = parseHash(hash);
        return "" + res[0] + "/" + res[1] + "/" + res[2];
    }

    @Override
    public GeoPoint hashAsGeoPoint(long hash) {
        int[] res = parseHash(hash);
        double tiles = Math.pow(2.0, res[0]);
        return new GeoPoint(tile2lat(res[2] + 0.5, tiles), tile2lon(res[1] + 0.5, tiles));
    }
}
