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

package org.elasticsearch.geo.geometry;

/**
 * Geometry-related utility methods
 */
public final class GeometryUtils {
    /**
     * Minimum longitude value.
     */
    static final double MIN_LON_INCL = -180.0D;

    /**
     * Maximum longitude value.
     */
    static final double MAX_LON_INCL = 180.0D;

    /**
     * Minimum latitude value.
     */
    static final double MIN_LAT_INCL = -90.0D;

    /**
     * Maximum latitude value.
     */
    static final double MAX_LAT_INCL = 90.0D;

    // No instance:
    private GeometryUtils() {
    }

    /**
     * validates latitude value is within standard +/-90 coordinate bounds
     */
    static void checkLatitude(double latitude) {
        if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
            throw new IllegalArgumentException(
                "invalid latitude " + latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL);
        }
    }

    /**
     * validates longitude value is within standard +/-180 coordinate bounds
     */
    static void checkLongitude(double longitude) {
        if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
            throw new IllegalArgumentException(
                "invalid longitude " + longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL);
        }
    }

    public static double checkAltitude(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new IllegalArgumentException("found Z value [" + zValue + "] but [ignore_z_value] "
                + "parameter is [" + ignoreZValue + "]");
        }
        return zValue;
    }

}
