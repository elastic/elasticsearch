/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.ESSloppyMath;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geometry.Rectangle;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLat;
import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;

/**
 * Implements geotile key hashing, same as used by many map tile implementations.
 * The string key is formatted as  "zoom/x/y"
 * The hash value (long) contains all three of those values compacted into a single 64bit value:
 *   bits 58..63 -- zoom (0..29)
 *   bits 29..57 -- X tile index (0..2^zoom)
 *   bits  0..28 -- Y tile index (0..2^zoom)
 */
public final class GeoTileUtils {

    private GeoTileUtils() {}

    private static final double PI_DIV_2 = Math.PI / 2;

    /**
     * Largest number of tiles (precision) to use.
     * This value cannot be more than (64-5)/2 = 29, because 5 bits are used for zoom level itself (0-31)
     * If zoom is not stored inside hash, it would be possible to use up to 32.
     * Note that changing this value will make serialization binary-incompatible between versions.
     * Another consideration is that index optimizes lat/lng storage, loosing some precision.
     * E.g. hash lng=140.74779717298918D lat=45.61884022447444D == "18/233561/93659", but shown as "18/233561/93658"
     */
    public static final int MAX_ZOOM = 29;

    /**
     * The geo-tile map is clipped at 85.05112878 to 90 and -85.05112878 to -90
     */
    public static final double LATITUDE_MASK = 85.0511287798066;

    /**
     * Since shapes are encoded, their boundaries are to be compared to against the encoded/decoded values of <code>LATITUDE_MASK</code>
     */
    public static final double NORMALIZED_LATITUDE_MASK = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(LATITUDE_MASK));
    public static final double NORMALIZED_NEGATIVE_LATITUDE_MASK =
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(-LATITUDE_MASK));

    /**
     * Bit position of the zoom value within hash - zoom is stored in the most significant 6 bits of a long number.
     */
    private static final int ZOOM_SHIFT = MAX_ZOOM * 2;

    /**
     * Bit mask to extract just the lowest 29 bits of a long
     */
    private static final long X_Y_VALUE_MASK = (1L << MAX_ZOOM) - 1;


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
    public static int checkPrecisionRange(int precision) {
        if (precision < 0 || precision > MAX_ZOOM) {
            throw new IllegalArgumentException("Invalid geotile_grid precision of " +
                precision + ". Must be between 0 and " + MAX_ZOOM + ".");
        }
        return precision;
    }

    /**
     * Calculates the x-coordinate in the tile grid for the specified longitude given
     * the number of tile columns for a pre-determined zoom-level.
     *
     * @param longitude the longitude to use when determining the tile x-coordinate
     * @param tiles     the number of tiles per row for a pre-determined zoom-level
     */
    public static int getXTile(double longitude, long tiles) {
        // normalizeLon treats this as 180, which is not friendly for tile mapping
        if (longitude == -180) {
            return 0;
        }

        int xTile = (int) Math.floor((normalizeLon(longitude) + 180) / 360 * tiles);

        // Edge values may generate invalid values, and need to be clipped.
        // For example, polar regions (above/below lat 85.05112878) get normalized.
        if (xTile < 0) {
            return 0;
        }
        if (xTile >= tiles) {
            return (int) tiles - 1;
        }

        return xTile;
    }

    /**
     * Calculates the y-coordinate in the tile grid for the specified longitude given
     * the number of tile rows for pre-determined zoom-level.
     *
     * @param latitude  the latitude to use when determining the tile y-coordinate
     * @param tiles     the number of tiles per column for a pre-determined zoom-level
     */
    public static int getYTile(double latitude, long tiles) {
        double latSin = SloppyMath.cos(PI_DIV_2 - Math.toRadians(normalizeLat(latitude)));
        int yTile = (int) Math.floor((0.5 - (Math.log((1 + latSin) / (1 - latSin)) / (4 * Math.PI))) * tiles);

        if (yTile < 0) {
            yTile = 0;
        }
        if (yTile >= tiles) {
            return (int) tiles - 1;
        }

        return yTile;
    }

    /**
     * Encode lon/lat to the geotile based long format.
     * The resulting hash contains interleaved tile X and Y coordinates.
     * The precision itself is also encoded as a few high bits.
     */
    public static long longEncode(double longitude, double latitude, int precision) {
        // Mathematics for this code was adapted from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java
        // Number of tiles for the current zoom level along the X and Y axis
        final long tiles = 1 << checkPrecisionRange(precision);
        long xTile = getXTile(longitude, tiles);
        long yTile = getYTile(latitude, tiles);
        return longEncodeTiles(precision, xTile, yTile);
    }

    /**
     * Encode a geotile hash style string to a long.
     *
     * @param hashAsString String in format "zoom/x/y"
     * @return long encoded value of the given string hash
     */
    public static long longEncode(String hashAsString) {
        int[] parsed = parseHash(hashAsString);
        return longEncode((long) parsed[0], (long) parsed[1], (long) parsed[2]);
    }

    public static long longEncodeTiles(int precision, long xTile, long yTile) {
        // Zoom value is placed in front of all the bits used for the geotile
        // e.g. when max zoom is 29, the largest index would use 58 bits (57th..0th),
        // leaving 5 bits unused for zoom. See MAX_ZOOM comment above.
        return ((long) precision << ZOOM_SHIFT) | (xTile << MAX_ZOOM) | yTile;
    }

    /**
     * Parse geotile hash as zoom, x, y integers.
     */
    private static int[] parseHash(long hash) {
        final int zoom = (int) (hash >>> ZOOM_SHIFT);
        final int xTile = (int) ((hash >>> MAX_ZOOM) & X_Y_VALUE_MASK);
        final int yTile = (int) (hash & X_Y_VALUE_MASK);
        return new int[]{zoom, xTile, yTile};
    }

    private static long longEncode(long precision, long xTile, long yTile) {
        // Zoom value is placed in front of all the bits used for the geotile
        // e.g. when max zoom is 29, the largest index would use 58 bits (57th..0th),
        // leaving 5 bits unused for zoom. See MAX_ZOOM comment above.
        return (precision << ZOOM_SHIFT) | (xTile << MAX_ZOOM) | yTile;
    }

    /**
     * Parse geotile String hash format in "zoom/x/y" into an array of integers
     */
    private static int[] parseHash(String hashAsString) {
        final String[] parts = hashAsString.split("/", 4);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid geotile_grid hash string of " +
                hashAsString + ". Must be three integers in a form \"zoom/x/y\".");
        }
        try {
            return new int[]{Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2])};
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid geotile_grid hash string of " +
                hashAsString + ". Must be three integers in a form \"zoom/x/y\".", e);
        }
    }

    /**
     * Encode to a geotile string from the geotile based long format
     */
    public static String stringEncode(long hash) {
        int[] res = parseHash(hash);
        validateZXY(res[0], res[1], res[2]);
        return "" + res[0] + "/" + res[1] + "/" + res[2];
    }

    /**
     * Decode long hash as a GeoPoint (center of the tile)
     */
    static GeoPoint hashToGeoPoint(long hash) {
        int[] res = parseHash(hash);
        return zxyToGeoPoint(res[0], res[1], res[2]);
    }

    /**
     * Decode a string bucket key in "zoom/x/y" format to a GeoPoint (center of the tile)
     */
    static GeoPoint keyToGeoPoint(String hashAsString) {
        int[] hashAsInts = parseHash(hashAsString);
        return zxyToGeoPoint(hashAsInts[0], hashAsInts[1], hashAsInts[2]);
    }

    public static Rectangle toBoundingBox(long hash) {
        int[] hashAsInts = parseHash(hash);
        return toBoundingBox(hashAsInts[1], hashAsInts[2], hashAsInts[0]);
    }

    /**
     * Decode a string bucket key in "zoom/x/y" format to a bounding box of the tile corners
     */
    public static Rectangle toBoundingBox(String hash) {
        int[] hashAsInts = parseHash(hash);
        return toBoundingBox(hashAsInts[1], hashAsInts[2], hashAsInts[0]);
    }

    public static Rectangle toBoundingBox(int xTile, int yTile, int precision) {
        final double tiles = validateZXY(precision, xTile, yTile);
        final double minN = Math.PI - (2.0 * Math.PI * (yTile + 1)) / tiles;
        final double maxN = Math.PI - (2.0 * Math.PI * (yTile)) / tiles;
        final double minY = Math.toDegrees(ESSloppyMath.atan(ESSloppyMath.sinh(minN)));
        final double minX = ((xTile) / tiles * 360.0) - 180;
        final double maxY = Math.toDegrees(ESSloppyMath.atan(ESSloppyMath.sinh(maxN)));
        final double maxX = ((xTile + 1) / tiles * 360.0) - 180;

        return new Rectangle(minX, maxX, maxY, minY);
    }

    /**
     * Validates Zoom, X, and Y values, and returns the total number of allowed tiles along the x/y axis.
     */
    private static int validateZXY(int zoom, int xTile, int yTile) {
        final int tiles = 1 << checkPrecisionRange(zoom);
        if (xTile < 0 || yTile < 0 || xTile >= tiles || yTile >= tiles) {
            throw new IllegalArgumentException(String.format(
                Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", zoom, xTile, yTile));
        }
        return tiles;
    }

    /**
     * Converts zoom/x/y integers into a GeoPoint.
     */
    private static GeoPoint zxyToGeoPoint(int zoom, int xTile, int yTile) {
        final int tiles = validateZXY(zoom, xTile, yTile);
        final double n = Math.PI - (2.0 * Math.PI * (yTile + 0.5)) / tiles;
        final double lat = Math.toDegrees(ESSloppyMath.atan(ESSloppyMath.sinh(n)));
        final double lon = ((xTile + 0.5) / tiles * 360.0) - 180;
        return new GeoPoint(lat, lon);
    }
}
