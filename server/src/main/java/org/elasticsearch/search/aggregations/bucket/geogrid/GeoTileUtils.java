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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;

/**
 * Implements quad key hashing, same as used by map tiles.
 * The string key is formatted as  "zoom/x/y"
 * The hash value (long) contains all three of those values.
 */
class GeoTileUtils {

    /**
     * Largest number of tiles (precision) to use.
     * This value cannot be more than (64-5)/2 = 29, because 5 bits are used for zoom level itself (0-31)
     * If zoom is not stored inside hash, it would be possible to use up to 32.
     * Another consideration is that index optimizes lat/lng storage, loosing some precision.
     * E.g. hash lng=140.74779717298918D lat=45.61884022447444D == "18/233561/93659", but shown as "18/233561/93658"
     */
    static final int MAX_ZOOM = 29;

    /**
     * Bit position of the zoom value within hash.  Must be &gt;= 2*MAX_ZOOM
     * Keeping it at a constant place allows MAX_ZOOM to be increased
     * without breaking serialization binary compatibility
     */
    private static final int ZOOM_SHIFT = MAX_ZOOM * 2;

    private static final long VALUE_MASK = (1L << MAX_ZOOM) - 1;

    /**
     * Parse an integer precision (zoom level). The {@link ValueType#INT} allows it to be a number or a string.
     *
     * The precision is expressed as a zoom level between 0 and {@link #MAX_ZOOM} (inclusive).
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return int representing precision
     */
    static int parsePrecision(XContentParser parser) throws IOException, ElasticsearchParseException {
        final Object node = parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER)
            ? Integer.valueOf(parser.intValue())
            : parser.text();
        return XContentMapValues.nodeIntegerValue(node);
    }

    /**
     * Assert the precision value is within the allowed range, and return it if ok, or throw.
     */
    static int checkPrecisionRange(int precision) {
        if (precision < 0 || precision > MAX_ZOOM) {
            throw new IllegalArgumentException("Invalid geotile_grid precision of " +
                precision + ". Must be between 0 and " + MAX_ZOOM + ".");
        }
        return precision;
    }

    /**
     * Encode lon/lat to the geotile based long format.
     * The resulting hash contains interleaved tile X and Y coordinates.
     * The precision itself is also encoded as a few high bits.
     */
    static long longEncode(double longitude, double latitude, int precision) {
        // Mathematics for this code was adapted from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

        // How many tiles in X and in Y
        final int tiles = 1 << checkPrecisionRange(precision);
        final double lon = normalizeLon(longitude);
        final double lat_rad = Math.toRadians(normalizeLat(latitude));

        int xTile = (int) Math.floor((lon + 180) / 360 * tiles);
        int yTile = (int) Math.floor((1 - Math.log(Math.tan(lat_rad) + 1 / Math.cos(lat_rad)) / Math.PI) / 2 * tiles);
        if (xTile < 0) {
            xTile = 0;
        }
        if (xTile >= tiles) {
            xTile = tiles - 1;
        }
        if (yTile < 0) {
            yTile = 0;
        }
        if (yTile >= tiles) {
            yTile = tiles - 1;
        }

        // Zoom value is placed in front of all the bits used for the geotile
        // e.g. when max zoom is 29, the largest index would use 58 bits (57th..0th),
        // leaving 5 bits unused for zoom. See MAX_ZOOM comment above.
        return ((long) precision << ZOOM_SHIFT) | ((long) xTile << MAX_ZOOM) | ((long) yTile);
    }

    /**
     * Parse geotile hash as zoom, x, y integers.
     */
    private static int[] parseHash(long hash) {
        final int zoom = (int) (hash >>> ZOOM_SHIFT);
        int xTile = (int) ((hash >>> MAX_ZOOM) & VALUE_MASK);
        int yTile = (int) (hash & VALUE_MASK);
        return new int[]{zoom, xTile, yTile};
    }

    /**
     * Encode to a geotile string from the geotile based long format
     */
    static String stringEncode(long hash) {
        int[] res = parseHash(hash);
        validateZXY(res[0], res[1], res[2]);
        return "" + res[0] + "/" + res[1] + "/" + res[2];
    }

    static GeoPoint hashToGeoPoint(long hash) {
        int[] res = parseHash(hash);
        return zxyToGeoPoint(res[0], res[1], res[2]);
    }

    static GeoPoint hashToGeoPoint(String hashAsString) {
        Throwable cause = null;
        try {
            final String[] parts = hashAsString.split("/", 4);
            if (parts.length == 3) {
                return zxyToGeoPoint(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
            }
        } catch (IllegalArgumentException e) {
            // This will also handle NumberFormatException
            cause = e;
        }
        throw new IllegalArgumentException("Invalid geotile_grid hash string of " +
            hashAsString + ". Must be three integers in a form \"zoom/x/y\".", cause);
    }

    private static int validateZXY(int zoom, int xTile, int yTile) {
        final int tiles = 1 << checkPrecisionRange(zoom);
        if (xTile < 0 || yTile < 0 || xTile >= tiles || yTile >= tiles) {
            throw new IllegalArgumentException("hash-tile");
        }
        return tiles;
    }

    private static GeoPoint zxyToGeoPoint(int zoom, int xTile, int yTile) {
        final int tiles = validateZXY(zoom, xTile, yTile);
        final double n = Math.PI - (2.0 * Math.PI * (yTile + 0.5)) / tiles;
        final double lat = Math.toDegrees(Math.atan(Math.sinh(n)));
        final double lon = ((xTile + 0.5) / tiles * 360.0) - 180;
        return new GeoPoint(lat, lon);
    }
}
