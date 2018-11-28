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

package org.elasticsearch.geo;

import org.apache.lucene.util.NumericUtils;

import static org.elasticsearch.geo.GeoUtils.MAX_LAT_INCL;
import static org.elasticsearch.geo.GeoUtils.MAX_LON_INCL;
import static org.elasticsearch.geo.GeoUtils.MIN_LON_INCL;
import static org.elasticsearch.geo.GeoUtils.MIN_LAT_INCL;
import static org.elasticsearch.geo.GeoUtils.checkLatitude;
import static org.elasticsearch.geo.GeoUtils.checkLongitude;

/**
 * reusable geopoint encoding methods
 */
public final class GeoEncodingUtils {
    /**
     * number of bits used for quantizing latitude and longitude values
     */
    public static final short BITS = 32;

    private static final double LAT_SCALE = (0x1L << BITS) / 180.0D;
    private static final double LAT_DECODE = 1 / LAT_SCALE;
    private static final double LON_SCALE = (0x1L << BITS) / 360.0D;
    private static final double LON_DECODE = 1 / LON_SCALE;

    // No instance:
    private GeoEncodingUtils() {
    }

    /**
     * Quantizes double (64 bit) latitude into 32 bits (rounding down: in the direction of -90)
     *
     * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
     * @return encoded value as a 32-bit {@code int}
     * @throws IllegalArgumentException if latitude is out of bounds
     */
    public static int encodeLatitude(double latitude) {
        checkLatitude(latitude);
        // the maximum possible value cannot be encoded without overflow
        if (latitude == 90.0D) {
            latitude = Math.nextDown(latitude);
        }
        return (int) Math.floor(latitude / LAT_DECODE);
    }

    /**
     * Quantizes double (64 bit) latitude into 32 bits (rounding up: in the direction of +90)
     *
     * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
     * @return encoded value as a 32-bit {@code int}
     * @throws IllegalArgumentException if latitude is out of bounds
     */
    public static int encodeLatitudeCeil(double latitude) {
        GeoUtils.checkLatitude(latitude);
        // the maximum possible value cannot be encoded without overflow
        if (latitude == 90.0D) {
            latitude = Math.nextDown(latitude);
        }
        return (int) Math.ceil(latitude / LAT_DECODE);
    }

    /**
     * Quantizes double (64 bit) longitude into 32 bits (rounding down: in the direction of -180)
     *
     * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
     * @return encoded value as a 32-bit {@code int}
     * @throws IllegalArgumentException if longitude is out of bounds
     */
    public static int encodeLongitude(double longitude) {
        checkLongitude(longitude);
        // the maximum possible value cannot be encoded without overflow
        if (longitude == 180.0D) {
            longitude = Math.nextDown(longitude);
        }
        return (int) Math.floor(longitude / LON_DECODE);
    }

    /**
     * Quantizes double (64 bit) longitude into 32 bits (rounding up: in the direction of +180)
     *
     * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
     * @return encoded value as a 32-bit {@code int}
     * @throws IllegalArgumentException if longitude is out of bounds
     */
    public static int encodeLongitudeCeil(double longitude) {
        GeoUtils.checkLongitude(longitude);
        // the maximum possible value cannot be encoded without overflow
        if (longitude == 180.0D) {
            longitude = Math.nextDown(longitude);
        }
        return (int) Math.ceil(longitude / LON_DECODE);
    }

    /**
     * Turns quantized value from {@link #encodeLatitude} back into a double.
     *
     * @param encoded encoded value: 32-bit quantized value.
     * @return decoded latitude value.
     */
    public static double decodeLatitude(int encoded) {
        double result = encoded * LAT_DECODE;
        assert result >= MIN_LAT_INCL && result < MAX_LAT_INCL;
        return result;
    }

    /**
     * Turns quantized value from byte array back into a double.
     *
     * @param src    byte array containing 4 bytes to decode at {@code offset}
     * @param offset offset into {@code src} to decode from.
     * @return decoded latitude value.
     */
    public static double decodeLatitude(byte[] src, int offset) {
        return decodeLatitude(NumericUtils.sortableBytesToInt(src, offset));
    }

    /**
     * Turns quantized value from {@link #encodeLongitude} back into a double.
     *
     * @param encoded encoded value: 32-bit quantized value.
     * @return decoded longitude value.
     */
    public static double decodeLongitude(int encoded) {
        double result = encoded * LON_DECODE;
        assert result >= MIN_LON_INCL && result < MAX_LON_INCL;
        return result;
    }

    /**
     * Turns quantized value from byte array back into a double.
     *
     * @param src    byte array containing 4 bytes to decode at {@code offset}
     * @param offset offset into {@code src} to decode from.
     * @return decoded longitude value.
     */
    public static double decodeLongitude(byte[] src, int offset) {
        return decodeLongitude(NumericUtils.sortableBytesToInt(src, offset));
    }
}
