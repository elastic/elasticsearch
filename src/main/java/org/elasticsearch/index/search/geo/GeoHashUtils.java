/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.search.geo;

import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Utilities for encoding and decoding geohashes. Based on
 * http://en.wikipedia.org/wiki/Geohash.
 */
// LUCENE MONITOR: monitor against spatial package
// replaced with native DECODE_MAP
public class GeoHashUtils {

    private static final char[] BASE_32 = {'0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

//    private final static Map<Character, Integer> DECODE_MAP = new HashMap<Character, Integer>();

    private final static TIntIntHashMap DECODE_MAP = new TIntIntHashMap();

    public static final int PRECISION = 12;
    private static final int[] BITS = {16, 8, 4, 2, 1};

    static {
        for (int i = 0; i < BASE_32.length; i++) {
            DECODE_MAP.put(BASE_32[i], i);
        }
    }

    private GeoHashUtils() {
    }

    public static String encode(double latitude, double longitude) {
        return encode(latitude, longitude, PRECISION);
    }

    /**
     * Encodes the given latitude and longitude into a geohash
     *
     * @param latitude  Latitude to encode
     * @param longitude Longitude to encode
     * @return Geohash encoding of the longitude and latitude
     */
    public static String encode(double latitude, double longitude, int precision) {
//        double[] latInterval = {-90.0, 90.0};
//        double[] lngInterval = {-180.0, 180.0};
        double latInterval0 = -90.0;
        double latInterval1 = 90.0;
        double lngInterval0 = -180.0;
        double lngInterval1 = 180.0;

        final StringBuilder geohash = new StringBuilder();
        boolean isEven = true;

        int bit = 0;
        int ch = 0;

        while (geohash.length() < precision) {
            double mid = 0.0;
            if (isEven) {
//                mid = (lngInterval[0] + lngInterval[1]) / 2D;
                mid = (lngInterval0 + lngInterval1) / 2D;
                if (longitude > mid) {
                    ch |= BITS[bit];
//                    lngInterval[0] = mid;
                    lngInterval0 = mid;
                } else {
//                    lngInterval[1] = mid;
                    lngInterval1 = mid;
                }
            } else {
//                mid = (latInterval[0] + latInterval[1]) / 2D;
                mid = (latInterval0 + latInterval1) / 2D;
                if (latitude > mid) {
                    ch |= BITS[bit];
//                    latInterval[0] = mid;
                    latInterval0 = mid;
                } else {
//                    latInterval[1] = mid;
                    latInterval1 = mid;
                }
            }

            isEven = !isEven;

            if (bit < 4) {
                bit++;
            } else {
                geohash.append(BASE_32[ch]);
                bit = 0;
                ch = 0;
            }
        }

        return geohash.toString();
    }

    public static double[] decode(String geohash) {
        double[] ret = new double[2];
        decode(geohash, ret);
        return ret;
    }

    /**
     * Decodes the given geohash into a latitude and longitude
     *
     * @param geohash Geohash to deocde
     * @return Array with the latitude at index 0, and longitude at index 1
     */
    public static void decode(String geohash, double[] ret) {
//        double[] latInterval = {-90.0, 90.0};
//        double[] lngInterval = {-180.0, 180.0};
        double latInterval0 = -90.0;
        double latInterval1 = 90.0;
        double lngInterval0 = -180.0;
        double lngInterval1 = 180.0;

        boolean isEven = true;

        for (int i = 0; i < geohash.length(); i++) {
            final int cd = DECODE_MAP.get(geohash.charAt(i));

            for (int mask : BITS) {
                if (isEven) {
                    if ((cd & mask) != 0) {
//                        lngInterval[0] = (lngInterval[0] + lngInterval[1]) / 2D;
                        lngInterval0 = (lngInterval0 + lngInterval1) / 2D;
                    } else {
//                        lngInterval[1] = (lngInterval[0] + lngInterval[1]) / 2D;
                        lngInterval1 = (lngInterval0 + lngInterval1) / 2D;
                    }
                } else {
                    if ((cd & mask) != 0) {
//                        latInterval[0] = (latInterval[0] + latInterval[1]) / 2D;
                        latInterval0 = (latInterval0 + latInterval1) / 2D;
                    } else {
//                        latInterval[1] = (latInterval[0] + latInterval[1]) / 2D;
                        latInterval1 = (latInterval0 + latInterval1) / 2D;
                    }
                }
                isEven = !isEven;
            }

        }
//        latitude = (latInterval[0] + latInterval[1]) / 2D;
        ret[0] = (latInterval0 + latInterval1) / 2D;
//        longitude = (lngInterval[0] + lngInterval[1]) / 2D;
        ret[1] = (lngInterval0 + lngInterval1) / 2D;

//        return ret;
    }
}