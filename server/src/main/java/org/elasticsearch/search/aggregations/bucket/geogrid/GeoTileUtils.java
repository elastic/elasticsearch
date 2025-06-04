/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.ESSloppyMath;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.geo.GeoUtils.quantizeLat;

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

    private static final double PI_DIV_2 = Math.PI / 2.0;

    private static final double PI_TIMES_2 = Math.PI * 2.0;

    private static final double PI_TIMES_4 = Math.PI * 4.0;

    // precision up to geometry and arithmetic solution are consistent
    private static final int MAX_TILES_FULL_PRECISION = 1 << 20;

    // lucene latitude resolution
    static final double LUCENE_LAT_RES = 180.0D / (0x1L << 32);

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

    public static final int ENCODED_LATITUDE_MASK = GeoEncodingUtils.encodeLatitude(LATITUDE_MASK);
    public static final int ENCODED_NEGATIVE_LATITUDE_MASK = GeoEncodingUtils.encodeLatitude(-LATITUDE_MASK);
    /**
     * Since shapes are encoded, their boundaries are to be compared to against the encoded/decoded values of <code>LATITUDE_MASK</code>
     */
    public static final double NORMALIZED_LATITUDE_MASK = GeoEncodingUtils.decodeLatitude(ENCODED_LATITUDE_MASK);
    public static final double NORMALIZED_NEGATIVE_LATITUDE_MASK = GeoEncodingUtils.decodeLatitude(ENCODED_NEGATIVE_LATITUDE_MASK);

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
            throw new IllegalArgumentException(
                "Invalid geotile_grid precision of " + precision + ". Must be between 0 and " + MAX_ZOOM + "."
            );
        }
        return precision;
    }

    /**
     * Calculates the x-coordinate in the tile grid for the specified longitude given
     * the number of tile columns for a pre-determined zoom-level.
     *
     * @param longitude the longitude to use when determining the tile x-coordinate. Longitude is in degrees
     *                  and must be between -180 and 180 degrees.
     * @param tiles     the number of tiles per row for a pre-determined zoom-level
     */
    public static int getXTile(double longitude, int tiles) {
        assert longitude >= -180 && longitude <= 180 : "Longitude must be between -180 and 180 degrees";
        final double xTile = (longitude + 180.0) / 360.0 * tiles;
        // Edge values may generate invalid values, and need to be clipped.
        return Math.max(0, Math.min(tiles - 1, (int) Math.floor(xTile)));
    }

    /**
     * Calculates the y-coordinate in the tile grid for the specified longitude given
     * the number of tile rows for pre-determined zoom-level.
     *
     * @param latitude  the latitude to use when determining the tile y-coordinate. Latitude is in degrees
     *                  and must be between -90 and 90 degrees.
     * @param tiles     the number of tiles per column for a pre-determined zoom-level
     */
    public static int getYTile(double latitude, int tiles) {
        assert latitude >= -90 && latitude <= 90 : "Latitude must be between -90 and 90 degrees";
        final double latSin = SloppyMath.cos(PI_DIV_2 - Math.toRadians(latitude));
        final double yTile = (0.5 - (ESSloppyMath.log((1.0 + latSin) / (1.0 - latSin)) / PI_TIMES_4)) * tiles;
        // Edge values may generate invalid values, and need to be clipped.
        // For example, polar regions (above/below lat 85.05112878) get normalized.
        return Math.max(0, Math.min(tiles - 1, (int) Math.floor(yTile)));
    }

    /**
     * Encode lon/lat to the geotile based long format.
     * The resulting hash contains interleaved tile X and Y coordinates.
     * The precision itself is also encoded as a few high bits.
     */
    public static long longEncode(double longitude, double latitude, int precision) {
        // Mathematics for this code was adapted from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java
        // Number of tiles for the current zoom level along the X and Y axis
        final int tiles = 1 << checkPrecisionRange(precision);
        return longEncodeTiles(precision, getXTile(longitude, tiles), getYTile(latitude, tiles));
    }

    /**
     * Encode a geotile hash style string to a long.
     *
     * @param hashAsString String in format "zoom/x/y"
     * @return long encoded value of the given string hash
     */
    public static long longEncode(String hashAsString) {
        final int[] parsed = parseHash(hashAsString);
        return longEncodeTiles(parsed[0], parsed[1], parsed[2]);
    }

    public static long longEncodeTiles(int precision, int xTile, int yTile) {
        // Zoom value is placed in front of all the bits used for the geotile
        // e.g. when max zoom is 29, the largest index would use 58 bits (57th..0th),
        // leaving 5 bits unused for zoom. See MAX_ZOOM comment above.
        return ((long) precision << ZOOM_SHIFT) | ((long) xTile << MAX_ZOOM) | yTile;
    }

    /**
     * Parse geotile hash as zoom, x, y integers.
     */
    private static int[] parseHash(long hash) {
        return new int[] { (int) (hash >>> ZOOM_SHIFT), (int) ((hash >>> MAX_ZOOM) & X_Y_VALUE_MASK), (int) (hash & X_Y_VALUE_MASK) };
    }

    /**
     * Parse geotile String hash format in "zoom/x/y" into an array of integers
     */
    public static int[] parseHash(String hashAsString) {
        final String[] parts = hashAsString.split("/", 4);
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                "Invalid geotile_grid hash string of " + hashAsString + ". Must be three integers in a form \"zoom/x/y\"."
            );
        }
        try {
            return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]) };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid geotile_grid hash string of " + hashAsString + ". Must be three integers in a form \"zoom/x/y\".",
                e
            );
        }
    }

    /**
     * Encode to a geotile string from the geotile based long format
     */
    public static String stringEncode(long hash) {
        final int[] res = parseHash(hash);
        validateZXY(res[0], res[1], res[2]);
        return res[0] + "/" + res[1] + "/" + res[2];
    }

    /**
     * Decode long hash as a GeoPoint (center of the tile)
     */
    static GeoPoint hashToGeoPoint(long hash) {
        final int[] res = parseHash(hash);
        return zxyToGeoPoint(res[0], res[1], res[2]);
    }

    /**
     * Decode a string bucket key in "zoom/x/y" format to a GeoPoint (center of the tile)
     */
    static GeoPoint keyToGeoPoint(String hashAsString) {
        final int[] hashAsInts = parseHash(hashAsString);
        return zxyToGeoPoint(hashAsInts[0], hashAsInts[1], hashAsInts[2]);
    }

    public static Rectangle toBoundingBox(long hash) {
        final int[] hashAsInts = parseHash(hash);
        return toBoundingBox(hashAsInts[1], hashAsInts[2], hashAsInts[0]);
    }

    /**
     * Decode a string bucket key in "zoom/x/y" format to a bounding box of the tile corners
     */
    public static Rectangle toBoundingBox(String hash) {
        final int[] hashAsInts = parseHash(hash);
        return toBoundingBox(hashAsInts[1], hashAsInts[2], hashAsInts[0]);
    }

    /**
     * Decode a bucket key to a bounding box of the tile corners. The points belonging
     * to the max latitude and min longitude belong to the tile while the points
     * belonging to the min latitude and max longitude belong to the next tile.
     */
    public static Rectangle toBoundingBox(int xTile, int yTile, int precision) {
        final int tiles = validateZXY(precision, xTile, yTile);
        return new Rectangle(
            tileToLon(xTile, tiles),            // minLon
            tileToLon(xTile + 1, tiles),  // maxLon
            tileToLat(yTile, tiles),            // maxLat
            tileToLat(yTile + 1, tiles)   // minLat
        );
    }

    /**
     * Decode a xTile into its longitude value
     */
    public static double tileToLon(int xTile, int tiles) {
        return tileToLon(xTile, (double) tiles);
    }

    private static double tileToLon(double xTile, double tiles) {
        return (xTile / tiles * 360.0) - 180.0;
    }

    /**
     * Decode a yTile into its latitude value
     */
    public static double tileToLat(int yTile, int tiles) {
        final double lat = tileToLat((double) yTile, tiles);
        if (tiles < MAX_TILES_FULL_PRECISION || yTile == 0 || yTile == tiles) {
            return lat; // precise case, don't need to do more work
        }
        // Maybe adjust latitude due to numerical errors
        final double qLat = quantizeLat(lat);
        final int computedYTile = getYTile(qLat, tiles);
        // the idea here is that the latitude returned belongs to the tile and the next latitude up belongs to the next tile
        // therefore we can be in the current tile and we need to find the point up just before the next tile,
        // or we are in the other tile and we need to find the first point down that belong to this tile.
        return findBoundaryPoint(qLat, computedYTile, tiles, computedYTile == yTile ? LUCENE_LAT_RES : -LUCENE_LAT_RES);
    }

    private static double findBoundaryPoint(double qLat, int yTile, int tiles, double step) {
        final double nextQLat = qLat + step;
        final int nextYTile = getYTile(nextQLat, tiles);
        if (yTile != nextYTile) {
            return step > 0 ? qLat : nextQLat;
        }
        return findBoundaryPoint(nextQLat, nextYTile, tiles, step);
    }

    private static double tileToLat(double yTile, int tiles) {
        final double n = Math.PI - (PI_TIMES_2 * yTile) / tiles;
        return Math.toDegrees(ESSloppyMath.atan(ESSloppyMath.sinh(n)));
    }

    /**
     * Validates Zoom, X, and Y values, and returns the total number of allowed tiles along the x/y axis.
     */
    private static int validateZXY(int zoom, int xTile, int yTile) {
        final int tiles = 1 << checkPrecisionRange(zoom);
        if (xTile < 0 || yTile < 0 || xTile >= tiles || yTile >= tiles) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Zoom/X/Y combination is not valid: %d/%d/%d", zoom, xTile, yTile)
            );
        }
        return tiles;
    }

    /**
     * Converts zoom/x/y integers into a GeoPoint.
     */
    private static GeoPoint zxyToGeoPoint(int zoom, int xTile, int yTile) {
        final int tiles = validateZXY(zoom, xTile, yTile);
        return new GeoPoint(tileToLat(yTile + 0.5, tiles), tileToLon(xTile + 0.5, tiles));
    }
}
