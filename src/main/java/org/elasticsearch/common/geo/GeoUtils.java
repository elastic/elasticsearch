/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 */
public class GeoUtils {

    /**
     * Normalize longitude to lie within the -180 (exclusive) to 180 (inclusive) range.
     *
     * @param lon Longitude to normalize
     * @return The normalized longitude.
     */
    public static double normalizeLon(double lon) {
        return centeredModulus(lon, 360);
    }

    /**
     * Normalize latitude to lie within the -90 to 90 (both inclusive) range.
     * <p/>
     * Note: You should not normalize longitude and latitude separately,
     * because when normalizing latitude it may be necessary to
     * add a shift of 180&deg; in the longitude.
     * For this purpose, you should call the
     * {@link #normalizePoint(GeoPoint)} function.
     *
     * @param lat Latitude to normalize
     * @return The normalized latitude.
     * @see #normalizePoint(GeoPoint)
     */
    public static double normalizeLat(double lat) {
        lat = centeredModulus(lat, 360);
        if (lat < -90) {
            lat = -180 - lat;
        } else if (lat > 90) {
            lat = 180 - lat;
        }
        return lat;
    }

    /**
     * Normalize the geo {@code Point} for its coordinates to lie within their
     * respective normalized ranges.
     * <p/>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     *
     * @param point The point to normalize in-place.
     */
    public static void normalizePoint(GeoPoint point) {
        normalizePoint(point, true, true);
    }

    /**
     * Normalize the geo {@code Point} for the given coordinates to lie within
     * their respective normalized ranges.
     * <p/>
     * You can control which coordinate gets normalized with the two flags.
     * <p/>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     * in order to normalize properly the latitude.
     * If normalizing latitude but not longitude, it is assumed that
     * the longitude is in the form x+k*360, with x in ]-180;180],
     * and k is meaningful to the application.
     * Therefore x will be adjusted while keeping k preserved.
     *
     * @param point   The point to normalize in-place.
     * @param normLat Whether to normalize latitude or leave it as is.
     * @param normLon Whether to normalize longitude.
     */
    public static void normalizePoint(GeoPoint point, boolean normLat, boolean normLon) {
        double lat = point.lat();
        double lon = point.lon();
        if (normLat) {
            lat = centeredModulus(lat, 360);
            boolean shift = true;
            if (lat < -90) {
                lat = -180 - lat;
            } else if (lat > 90) {
                lat = 180 - lat;
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    lon += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    lon += normalizeLon(lon) > 0 ? -180 : 180;
                }
            }
        }
        if (normLon) {
            lon = centeredModulus(lon, 360);
        }
        point.reset(lat, lon);
    }

    private static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0) {
            rtn += divisor;
        }
        if (rtn > divisor / 2) {
            rtn -= divisor;
        }
        return rtn;
    }

}
