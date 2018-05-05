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

import com.google.openlocationcode.OpenLocationCode;
import org.elasticsearch.common.geo.GeoPoint;

import java.util.Arrays;

public class GeoPlusCodeHandler implements GeoHashTypeProvider {

    /**
     * List of all allowed precisions. Note that 21^14 is the largest value
     * that can fit within a long value, so we could used up to length 14.
     */
    public static final int[] ALLOWED_LENGTHS = {4, 6, 8, 10, 11, 12, 13, 14};

    /**
     * Maximum plus code length (without the '+' symbol) that we support.
     */
    public static final int MAX_LENGTH = ALLOWED_LENGTHS[ALLOWED_LENGTHS.length - 1];

    /**
     * Same as official plus code alphabet, but also includes "0" to preserve the code length
     */
    private static final String ALPHABET0 = "023456789CFGHJMPQRVWX";
    /**
     * Length of the extended alphabet (21)
     */
    private static final int ALPHABET0_SIZE = ALPHABET0.length();

    /**
     * Initialize ALPHABET0_LOOKUP table for quick O(1) lookup of alphabet letters -> int
     * There is some wasted space (first 32 values, and a few gaps), but results is slightly better perf
     */
    private static final int[] ALPHABET0_LOOKUP;

    static {
        int size = GeoPlusCodeHandler.ALPHABET0_SIZE;
        int[] lookup = new int[GeoPlusCodeHandler.ALPHABET0.charAt(size - 1) + 1];
        Arrays.fill(lookup, -1);
        for (int i = 0; i < size; i++) {
            lookup[GeoPlusCodeHandler.ALPHABET0.charAt(i)] = i;
        }
        ALPHABET0_LOOKUP = lookup;
    }


    /**
     * Convert latitude+longitude to the plus code of a given length
     */
    public static String encodeCoordinates(final double lon, final double lat, final int codeLength) {
        return new OpenLocationCode(lat, lon, codeLength).getCode();
    }

    /**
     * Decode plus code hash back into a string
     */
    private static String decodePlusCode(final long hash) {

        StringBuilder result = new StringBuilder(MAX_LENGTH + 1);

        long rest = hash;
        while (rest > 0) {
            long val = rest % ALPHABET0_SIZE;
            result.append(ALPHABET0.charAt((int) val));
            rest = rest / ALPHABET0_SIZE;
        }

        result.reverse();
        result.insert(8, '+');

        return result.toString();
    }

    @Override
    public int getDefaultPrecision() {
        // default 6 == 0.0025 degrees ~~ 5km at equator
        return 6;
    }

    @Override
    public int parsePrecisionString(String precision) {
        try {
            // we want to treat simple integer strings as precision levels, not distances
            return Integer.parseInt(precision);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Non-integer precision is not supported for plus codes");
        }
    }

    @Override
    public int validatePrecision(int precision) {
        if ((precision < 4) || (precision > MAX_LENGTH) ||
            (precision < OpenLocationCode.CODE_PRECISION_NORMAL && precision % 2 == 1)
            ) {
            throw new IllegalArgumentException("Invalid geohash pluscode aggregation precision of " + precision
                + ". Must be between 4 and " + MAX_LENGTH + ", and must be even if less than 8.");
        }
        return precision;
    }

    @Override
    public long calculateHash(double longitude, double latitude, int precision) {

        String pluscode = encodeCoordinates(longitude, latitude, precision);

        long result = 0;
        for (int i = 0; i < pluscode.length(); i++) {
            char ch = pluscode.charAt(i);
            if (ch == '+') continue;
            int pos = ALPHABET0_LOOKUP[ch];
            if (pos < 0) {
                throw new IllegalArgumentException("Character '" + ch + "' is not a valid plus code");
            }
            result = result * ALPHABET0_SIZE + pos;
        }
        return result;
    }

    @Override
    public String hashAsString(long hash) {
        return decodePlusCode(hash);
    }

    @Override
    public GeoPoint hashAsObject(long hash) {
        OpenLocationCode.CodeArea area = new OpenLocationCode(decodePlusCode(hash)).decode();
        return new GeoPoint(area.getCenterLatitude(), area.getCenterLongitude());
    }
}
