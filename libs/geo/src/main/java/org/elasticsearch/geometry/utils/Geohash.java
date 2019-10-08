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
package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Utilities for converting to/from the GeoHash standard
 *
 * The geohash long format is represented as lon/lat (x/y) interleaved with the 4 least significant bits
 * representing the level (1-12) [xyxy...xyxyllll]
 *
 * This differs from a morton encoded value which interleaves lat/lon (y/x).
 *
 * NOTE: this will replace {@code org.elasticsearch.common.geo.GeoHashUtils}
 */
public class Geohash {
    private static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    private static final String BASE_32_STRING = new String(BASE_32);
    /** maximum precision for geohash strings */
    public static final int PRECISION = 12;
    /** number of bits used for quantizing latitude and longitude values */
    private static final short BITS = 32;
    private static final double LAT_SCALE = (0x1L<<(BITS-1))/180.0D;
    private static final double LAT_DECODE = 180.0D/(0x1L<<BITS);
    private static final double LON_SCALE = (0x1L<<(BITS-1))/360.0D;
    private static final double LON_DECODE = 360.0D/(0x1L<<BITS);

    private static final short MORTON_OFFSET = (BITS<<1) - (PRECISION*5);
    /** Bit encoded representation of the latitude of north pole */
    private static final long MAX_LAT_BITS = (0x1L << (PRECISION * 5 / 2)) - 1;


    // no instance:
    private Geohash() {
    }

    /** Returns a {@link Point} instance from a geohash string */
    public static Point toPoint(final String geohash) throws IllegalArgumentException {
        final long hash = mortonEncode(geohash);
        return new Point(decodeLongitude(hash), decodeLatitude(hash));
    }

    /**
     * Computes the bounding box coordinates from a given geohash
     *
     * @param geohash Geohash of the defined cell
     * @return GeoRect rectangle defining the bounding box
     */
    public static Rectangle toBoundingBox(final String geohash) {
        // bottom left is the coordinate
        Point bottomLeft = toPoint(geohash);
        int len = Math.min(12, geohash.length());
        long ghLong = longEncode(geohash, len);
        // shift away the level
        ghLong >>>= 4;
        // deinterleave
        long lon = BitUtil.deinterleave(ghLong >>> 1);
        long lat = BitUtil.deinterleave(ghLong);
        final int shift = (12 - len) * 5 + 2;
        if (lat < MAX_LAT_BITS) {
            // add 1 to lat and lon to get topRight
            ghLong = BitUtil.interleave((int)(lat + 1), (int)(lon + 1)) << 4 | len;
            final long mortonHash = BitUtil.flipFlop((ghLong >>> 4) << shift);
            Point topRight = new Point(decodeLongitude(mortonHash), decodeLatitude(mortonHash));
            return new Rectangle(bottomLeft.getX(), topRight.getX(), topRight.getY(), bottomLeft.getY());
        } else {
            // We cannot go north of north pole, so just using 90 degrees instead of calculating it using
            // add 1 to lon to get lon of topRight, we are going to use 90 for lat
            ghLong = BitUtil.interleave((int)lat, (int)(lon + 1)) << 4 | len;
            final long mortonHash = BitUtil.flipFlop((ghLong >>> 4) << shift);
            Point topRight = new Point(decodeLongitude(mortonHash), decodeLatitude(mortonHash));
            return new Rectangle(bottomLeft.getX(), topRight.getX(), 90D, bottomLeft.getY());
        }
    }

    /**
     * Calculate all neighbors of a given geohash cell.
     *
     * @param geohash Geohash of the defined cell
     * @return geohashes of all neighbor cells
     */
    public static Collection<? extends CharSequence> getNeighbors(String geohash) {
        return addNeighborsAtLevel(geohash, geohash.length(), new ArrayList<CharSequence>(8));
    }
    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighbors(String geohash, E neighbors) {
        return addNeighborsAtLevel(geohash, geohash.length(), neighbors);
    }

    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param level    level of the given geohash
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighborsAtLevel(String geohash,
                                                                                     int level, E neighbors) {
        String south = getNeighbor(geohash, level, 0, -1);
        String north = getNeighbor(geohash, level, 0, +1);
        if (north != null) {
            neighbors.add(getNeighbor(north, level, -1, 0));
            neighbors.add(north);
            neighbors.add(getNeighbor(north, level, +1, 0));
        }

        neighbors.add(getNeighbor(geohash, level, -1, 0));
        neighbors.add(getNeighbor(geohash, level, +1, 0));

        if (south != null) {
            neighbors.add(getNeighbor(south, level, -1, 0));
            neighbors.add(south);
            neighbors.add(getNeighbor(south, level, +1, 0));
        }

        return neighbors;
    }

    /**
     * Calculate the geohash of a neighbor of a geohash
     *
     * @param geohash the geohash of a cell
     * @param level   level of the geohash
     * @param dx      delta of the first grid coordinate (must be -1, 0 or +1)
     * @param dy      delta of the second grid coordinate (must be -1, 0 or +1)
     * @return geohash of the defined cell
     */
    public static final String getNeighbor(String geohash, int level, int dx, int dy) {
        int cell = BASE_32_STRING.indexOf(geohash.charAt(level -1));

        // Decoding the Geohash bit pattern to determine grid coordinates
        int x0 = cell & 1;  // first bit of x
        int y0 = cell & 2;  // first bit of y
        int x1 = cell & 4;  // second bit of x
        int y1 = cell & 8;  // second bit of y
        int x2 = cell & 16; // third bit of x

        // combine the bitpattern to grid coordinates.
        // note that the semantics of x and y are swapping
        // on each level
        int x = x0 + (x1 / 2) + (x2 / 4);
        int y = (y0 / 2) + (y1 / 4);

        if (level == 1) {
            // Root cells at north (namely "bcfguvyz") or at
            // south (namely "0145hjnp") do not have neighbors
            // in north/south direction
            if ((dy < 0 && y == 0) || (dy > 0 && y == 3)) {
                return null;
            } else {
                return Character.toString(encodeBase32(x + dx, y + dy));
            }
        } else {
            // define grid coordinates for next level
            final int nx = ((level % 2) == 1) ? (x + dx) : (x + dy);
            final int ny = ((level % 2) == 1) ? (y + dy) : (y + dx);

            // if the defined neighbor has the same parent a the current cell
            // encode the cell directly. Otherwise find the cell next to this
            // cell recursively. Since encoding wraps around within a cell
            // it can be encoded here.
            // xLimit and YLimit must always be respectively 7 and 3
            // since x and y semantics are swapping on each level.
            if (nx >= 0 && nx <= 7 && ny >= 0 && ny <= 3) {
                return geohash.substring(0, level - 1) + encodeBase32(nx, ny);
            } else {
                String neighbor = getNeighbor(geohash, level - 1, dx, dy);
                return (neighbor != null) ? neighbor + encodeBase32(nx, ny) : neighbor;
            }
        }
    }

    /**
     * Encode lon/lat to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    public static final long longEncode(final double lon, final double lat, final int level) {
        // shift to appropriate level
        final short msf = (short)(((12 - level) * 5) + (MORTON_OFFSET - 2));
        return ((encodeLatLon(lat, lon) >>> msf) << 4) | level;
    }

    /**
     * Encode to a geohash string from full resolution longitude, latitude)
     */
    public static final String stringEncode(final double lon, final double lat) {
        return stringEncode(lon, lat, 12);
    }

    /**
     * Encode to a level specific geohash string from full resolution longitude, latitude
     */
    public static final String stringEncode(final double lon, final double lat, final int level) {
        // convert to geohashlong
        long interleaved = encodeLatLon(lat, lon);
        interleaved >>>= (((PRECISION - level) * 5) + (MORTON_OFFSET - 2));
        final long geohash = (interleaved << 4) | level;
        return stringEncode(geohash);
    }

    /**
     * Encode to a geohash string from the geohash based long format
     */
    public static final String stringEncode(long geoHashLong) {
        int level = (int)geoHashLong&15;
        geoHashLong >>>= 4;
        char[] chars = new char[level];
        do {
            chars[--level] = BASE_32[(int) (geoHashLong&31L)];
            geoHashLong>>>=5;
        } while(level > 0);

        return new String(chars);
    }

    /** base32 encode at the given grid coordinate */
    private static char encodeBase32(int x, int y) {
        return BASE_32[((x & 1) + ((y & 1) * 2) + ((x & 2) * 2) + ((y & 2) * 4) + ((x & 4) * 4)) % 32];
    }

    /**
     * Encode from geohash string to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    private static long longEncode(final String hash, int length) {
        int level = length - 1;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            l |= (b<<(level--*5));
            if (level < 0) {
                // We cannot handle more than 12 levels
                break;
            }
        }
        return (l << 4) | length;
    }

    /**
     * Encode to a morton long value from a given geohash string
     */
    public static long mortonEncode(final String hash) {
        if (hash.isEmpty()) {
            throw new IllegalArgumentException("empty geohash");
        }
        int level = 11;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            if (b < 0) {
                throw new IllegalArgumentException("unsupported symbol [" + c + "] in geohash [" + hash + "]");
            }
            l |= (b<<((level--*5) + (MORTON_OFFSET - 2)));
            if (level < 0) {
                // We cannot handle more than 12 levels
                break;
            }
        }
        return BitUtil.flipFlop(l);
    }

    private static long encodeLatLon(final double lat, final double lon) {
        // encode lat/lon flipping the sign bit so negative ints sort before positive ints
        final int latEnc = encodeLatitude(lat) ^ 0x80000000;
        final int lonEnc = encodeLongitude(lon) ^ 0x80000000;
        return BitUtil.interleave(latEnc, lonEnc) >>> 2;
    }


    /** encode latitude to integer */
    public static int encodeLatitude(double latitude) {
        // the maximum possible value cannot be encoded without overflow
        if (latitude == 90.0D) {
            latitude = Math.nextDown(latitude);
        }
        return (int) Math.floor(latitude / LAT_DECODE);
    }

    /** encode longitude to integer */
    public static int encodeLongitude(double longitude) {
        // the maximum possible value cannot be encoded without overflow
        if (longitude == 180.0D) {
            longitude = Math.nextDown(longitude);
        }
        return (int) Math.floor(longitude / LON_DECODE);
    }

    /** returns the latitude value from the string based geohash */
    public static final double decodeLatitude(final String geohash) {
        return decodeLatitude(Geohash.mortonEncode(geohash));
    }

    /** returns the latitude value from the string based geohash */
    public static final double decodeLongitude(final String geohash) {
        return decodeLongitude(Geohash.mortonEncode(geohash));
    }

    /** decode longitude value from morton encoded geo point */
    public static double decodeLongitude(final long hash) {
        return unscaleLon(BitUtil.deinterleave(hash));
    }

    /** decode latitude value from morton encoded geo point */
    public static double decodeLatitude(final long hash) {
        return unscaleLat(BitUtil.deinterleave(hash >>> 1));
    }

    private static double unscaleLon(final long val) {
        return (val / LON_SCALE) - 180;
    }

    private static double unscaleLat(final long val) {
        return (val / LAT_SCALE) - 90;
    }
}
