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

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.spatial.util.MortonEncoder;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.geo.utils.Geohash;

/**
 * Utilities for converting to/from the GeoHash standard
 *
 * The geohash long format is represented as lon/lat (x/y) interleaved with the 4 least significant bits
 * representing the level (1-12) [xyxy...xyxyllll]
 *
 * This differs from a morton encoded value which interleaves lat/lon (y/x).*
 */
public class GeoHashUtils {
    private static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    private static final String BASE_32_STRING = new String(BASE_32);

    /** maximum precision for geohash strings */
    public static final int PRECISION = 12;
    /** number of bits used for quantizing latitude and longitude values */
    public static final short BITS = 31;
    /** scaling factors to convert lat/lon into unsigned space */
    private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
    private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
    private static final short MORTON_OFFSET = (BITS<<1) - (PRECISION*5);

    // No instance:
    private GeoHashUtils() {
    }

    /*************************
     * 31 bit encoding utils *
     *************************/
    public static long encodeLatLon(final double lat, final double lon) {
      return MortonEncoder.encode(lat, lon) >>> 2;
    }

    /**
     * Encode lon/lat to the geohash based long format (lon/lat interleaved, 4 least significant bits = level)
     */
    public static final long longEncode(final double lon, final double lat, final int level) {
        // shift to appropriate level
        final short msf = (short)(((12 - level) * 5) + MORTON_OFFSET);
        return ((BitUtil.flipFlop(encodeLatLon(lat, lon)) >>> msf) << 4) | level;
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
        long morton = encodeLatLon(lat, lon);
        long mFlipped = BitUtil.flipFlop(morton);
        mFlipped >>>= (((PRECISION - level) * 5) + MORTON_OFFSET);
        final long geohash = (mFlipped << 4) | level;
        return stringEncode(geohash);
    }

    /**
     * Encode to a morton long value from a given geohash string
     */
    public static final long mortonEncode(final String hash) {
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
            l |= (b<<((level--*5) + MORTON_OFFSET));
            if (level < 0) {
                // We cannot handle more than 12 levels
                break;
            }
        }
        return BitUtil.flipFlop(l);
    }

    /**
     * Encode to a morton long value from a given geohash long value
     */
    public static final long mortonEncode(final long geoHashLong) {
        final int level = (int)(geoHashLong&15);
        final short odd = (short)(level & 1);

        return BitUtil.flipFlop(((geoHashLong >>> 4) << odd) << (((12 - level) * 5) + (MORTON_OFFSET - odd)));
    }

    /**
     * Computes the bounding box coordinates from a given geohash
     *
     * @param geohash Geohash of the defined cell
     * @return GeoRect rectangle defining the bounding box
     *
     * @deprecated use Geohash.toBoundingBox instead
     */
    @Deprecated
    public static Rectangle bbox(final String geohash) {
        org.elasticsearch.geo.geometry.Rectangle r = Geohash.toBoundingBox(geohash);
        return new Rectangle(r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
    }

    /** decode longitude value from morton encoded geo point */
    public static final double decodeLongitude(final long hash) {
      return unscaleLon(BitUtil.deinterleave(hash));
    }

    /** decode latitude value from morton encoded geo point */
    public static final double decodeLatitude(final long hash) {
      return unscaleLat(BitUtil.deinterleave(hash >>> 1));
    }

    private static double unscaleLon(final long val) {
      return (val / LON_SCALE) - 180;
    }

    private static double unscaleLat(final long val) {
      return (val / LAT_SCALE) - 90;
    }

    /** returns the latitude value from the string based geohash */
    public static final double decodeLatitude(final String geohash) {
        return decodeLatitude(mortonEncode(geohash));
    }

    /** returns the latitude value from the string based geohash */
    public static final double decodeLongitude(final String geohash) {
        return decodeLongitude(mortonEncode(geohash));
    }
}
