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

package org.apache.lucene.util;

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
 * @lucene.experimental
 */
public class XGeoHashUtils {
    public static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    public static final String BASE_32_STRING = new String(BASE_32);

    public static final int PRECISION = 12;
    private static final short MORTON_OFFSET = (XGeoUtils.BITS<<1) - (PRECISION*5);

    /**
     * Encode lon/lat to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    public static final long longEncode(final double lon, final double lat, final int level) {
        // shift to appropriate level
        final short msf = (short)(((12 - level) * 5) + MORTON_OFFSET);
        return ((BitUtil.flipFlop(XGeoUtils.mortonHash(lon, lat)) >>> msf) << 4) | level;
    }

    /**
     * Encode from geohash string to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    public static final long longEncode(final String hash) {
        int level = hash.length()-1;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            l |= (b<<(level--*5));
        }
        return (l<<4)|hash.length();
    }

    /**
     * Encode an existing geohash long to the provided precision
     */
    public static long longEncode(long geohash, int level) {
        final short precision = (short)(geohash & 15);
        if (precision == level) {
            return geohash;
        } else if (precision > level) {
            return ((geohash >>> (((precision - level) * 5) + 4)) << 4) | level;
        }
        return ((geohash >>> 4) << (((level - precision) * 5) + 4) | level);
    }

    /**
     * Encode to a geohash string from the geohash based long format
     */
    public static final String stringEncode(long geoHashLong) {
        int level = (int)geoHashLong&15;
        geoHashLong >>>= 4;
        char[] chars = new char[level];
        do {
            chars[--level] = BASE_32[(int)(geoHashLong&31L)];
            geoHashLong>>>=5;
        } while(level > 0);

        return new String(chars);
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
        // bit twiddle to geohash (since geohash is a swapped (lon/lat) encoding)
        final long hashedVal = BitUtil.flipFlop(XGeoUtils.mortonHash(lon, lat));

        StringBuilder geoHash = new StringBuilder();
        short precision = 0;
        final short msf = (XGeoUtils.BITS<<1)-5;
        long mask = 31L<<msf;
        do {
            geoHash.append(BASE_32[(int)((mask & hashedVal)>>>(msf-(precision*5)))]);
            // next 5 bits
            mask >>>= 5;
        } while (++precision < level);
        return geoHash.toString();
    }

    /**
     * Encode to a full precision geohash string from a given morton encoded long value
     */
    public static final String stringEncodeFromMortonLong(final long hashedVal) throws Exception {
        return stringEncode(hashedVal, PRECISION);
    }

    /**
     * Encode to a geohash string at a given level from a morton long
     */
    public static final String stringEncodeFromMortonLong(long hashedVal, final int level) {
        // bit twiddle to geohash (since geohash is a swapped (lon/lat) encoding)
        hashedVal = BitUtil.flipFlop(hashedVal);

        StringBuilder geoHash = new StringBuilder();
        short precision = 0;
        final short msf = (XGeoUtils.BITS<<1)-5;
        long mask = 31L<<msf;
        do {
            geoHash.append(BASE_32[(int)((mask & hashedVal)>>>(msf-(precision*5)))]);
            // next 5 bits
            mask >>>= 5;
        } while (++precision < level);
        return geoHash.toString();
    }

    /**
     * Encode to a morton long value from a given geohash string
     */
    public static final long mortonEncode(final String hash) {
        int level = 11;
        long b;
        long l = 0L;
        for(char c : hash.toCharArray()) {
            b = (long)(BASE_32_STRING.indexOf(c));
            l |= (b<<((level--*5) + MORTON_OFFSET));
        }
        return BitUtil.flipFlop(l);
    }

    /**
     * Encode to a morton long value from a given geohash long value
     */
    public static final long mortonEncode(final long geoHashLong) {
        final int level = (int)(geoHashLong&15);
        final short odd = (short)(level & 1);

        return BitUtil.flipFlop((geoHashLong >>> 4) << odd) << (((12 - level) * 5) + (MORTON_OFFSET - odd));
    }

    private static final char encode(int x, int y) {
        return BASE_32[((x & 1) + ((y & 1) * 2) + ((x & 2) * 2) + ((y & 2) * 4) + ((x & 4) * 4)) % 32];
    }

    /**
     * Calculate all neighbors of a given geohash cell.
     *
     * @param geohash Geohash of the defined cell
     * @return geohashes of all neighbor cells
     */
    public static Collection<? extends CharSequence> neighbors(String geohash) {
        return addNeighbors(geohash, geohash.length(), new ArrayList<CharSequence>(8));
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
    private final static String neighbor(String geohash, int level, int dx, int dy) {
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
                return Character.toString(encode(x + dx, y + dy));
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
                return geohash.substring(0, level - 1) + encode(nx, ny);
            } else {
                String neighbor = neighbor(geohash, level - 1, dx, dy);
                return (neighbor != null) ? neighbor + encode(nx, ny) : neighbor;
            }
        }
    }

    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighbors(String geohash, E neighbors) {
        return addNeighbors(geohash, geohash.length(), neighbors);
    }

    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param length    level of the given geohash
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighbors(String geohash, int length, E neighbors) {
        String south = neighbor(geohash, length, 0, -1);
        String north = neighbor(geohash, length, 0, +1);
        if (north != null) {
            neighbors.add(neighbor(north, length, -1, 0));
            neighbors.add(north);
            neighbors.add(neighbor(north, length, +1, 0));
        }

        neighbors.add(neighbor(geohash, length, -1, 0));
        neighbors.add(neighbor(geohash, length, +1, 0));

        if (south != null) {
            neighbors.add(neighbor(south, length, -1, 0));
            neighbors.add(south);
            neighbors.add(neighbor(south, length, +1, 0));
        }

        return neighbors;
    }
}