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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;

/**
 * Implements quad key hashing, same as used by map tiles.
 * The string key is formatted as  "zoom/x/y"
 * The hash value (long) contains all three of those values.
 */
public class GeoTileUtils {

    /**
     * Largest number of tiles (precision) to use.
     * This value cannot be more than (64-5)/2 = 29, because 5 bits are used for zoom level itself
     * If zoom is not stored inside hash, it would be possible to use up to 32.
     * Another consideration is that index optimizes lat/lng storage, loosing some precision.
     * E.g. hash lng=140.74779717298918D lat=45.61884022447444D == "18/233561/93659", but shown as "18/233561/93658"
     */
    public static final int MAX_ZOOM = 29;

    /**
     * Bit position of the zoom value within hash.  Must be &gt;= 2*MAX_ZOOM
     * Keeping it at a constant place allows MAX_ZOOM to be increased
     * without breaking serialization binary compatibility
     * (still, the newer version should not use higher MAX_ZOOM in the mixed cases)
     */
    private static final int ZOOM_SHIFT = 29 * 2;

    /**
     * Mask of all the bits used by the geotile in a hash
     */
    private static final long GEOTILE_MASK = (1L << ZOOM_SHIFT) - 1;

    /**
     * Parse geotile hash as zoom, x, y integers.
     */
    private static int[] parseHash(final long hash) {
        final int zoom = checkPrecisionRange((int) (hash >>> ZOOM_SHIFT));
        final int tiles = 1 << zoom;

        // decode the geotile bits as interleaved xTile and yTile
        long val = hash & GEOTILE_MASK;
        int xTile = (int) BitUtil.deinterleave(val);
        int yTile = (int) BitUtil.deinterleave(val >>> 1);
        if (xTile < 0 || yTile < 0 || xTile >= tiles || yTile >= tiles) {
            throw new IllegalArgumentException("hash-tile");
        }

        return new int[]{zoom, xTile, yTile};
    }

    /**
     * Parse an integer precision (zoom level). The {@link ValueType#INT} allows it to be a number or a string.
     *
     * The precision is expressed as a zoom level between 0 and {@link #MAX_ZOOM} (inclusive).
     *
     * @param parser {@link XContentParser} to parse the value from
     * @return int representing precision
     */
    public static int parsePrecision(XContentParser parser) throws IOException, ElasticsearchParseException {
        final Object node = parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER)
            ? Integer.valueOf(parser.intValue())
            : parser.text();
        return XContentMapValues.nodeIntegerValue(node);
    }

    /**
     * Assert the precision value is within the allowed range, and return it if ok, or throw.
     */
    public static int checkPrecisionRange(int precision) {
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
    public static long longEncode(double longitude, double latitude, int precision) {
        // Mathematics for this code was adapted from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

        // How many tiles in X and in Y
        final int tiles = 1 << checkPrecisionRange(precision);
        final double lon = normalizeLon(longitude);
        final double lat = normalizeLat(latitude);

        int xTile = (int) Math.floor((lon + 180) / 360 * tiles);
        int yTile = (int) Math.floor(
            (1 - Math.log(
                Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))
            ) / Math.PI) / 2 * tiles);
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
        // e.g. if max zoom is 26, the largest index would use 52 bits (51st..0th),
        // leaving 12 bits unused for zoom. See MAX_ZOOM comment above.
        return BitUtil.interleave(xTile, yTile) | ((long) precision << ZOOM_SHIFT);
    }

    /**
     * Encode to a geotile string from the geotile based long format
     */
    public static String stringEncode(long hash) {
        int[] res = parseHash(hash);
        return "" + res[0] + "/" + res[1] + "/" + res[2];
    }

    public static GeoPoint hashToGeoPoint(long hash) {
        int[] res = parseHash(hash);
        return zxyToGeoPoint(res[0], res[1], res[2]);
    }

    public static GeoPoint hashToGeoPoint(String hashAsString) {
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

    private static GeoPoint zxyToGeoPoint(int zoom, int xTile, int yTile) {
        final int maxTiles = 1 << checkPrecisionRange(zoom);
        if (xTile >= 0 && xTile < maxTiles && yTile >= 0 && yTile < maxTiles) {
            final double tiles = Math.pow(2.0, zoom);
            final double n = Math.PI - (2.0 * Math.PI * (yTile + 0.5)) / tiles;
            final double lat = Math.toDegrees(Math.atan(Math.sinh(n)));
            final double lon = ((xTile + 0.5) / tiles * 360.0) - 180;
            return new GeoPoint(lat, lon);
        }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "Invalid geotile_grid z/x/y values of %s/%s/%s", zoom, xTile, yTile));
    }
}
