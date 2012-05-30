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

package org.elasticsearch.index.search.geo;

import java.util.List;
import java.util.ArrayList;

/**
 */
public class GeoUtils {

    /**
     * Normalize longitude to lie within the
     * -180 (exclusive) to 180 (inclusive) range.
     *
     * @see normalizePoint(Point)
     * @return The normalized longitude.
     */
    public static double normalizeLon(double lon) {
        return centeredModulus(lon, 360);
    }

    /**
     * Normalize latitude to lie within the
     * -90 to 90 (both inclusive) range.
     * <p>
     * Note: You should not normalize longitude and latitude separately,
     *       because when normalizing latitude it may be necessary to
     *       add a shift of 180&deg; in the longitude.
     *       For this purpose, you should call the
     *       {@link normalizePoint(Point)} function.
     *
     * @see normalizePoint(Point)
     * @see normalizingLatImpliesShift(double)
     * @return The normalized latitude.
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
     * Returns {@code true} if normalizing the given latitude would
     * imply a 180&deg; shift in longitude.
     *
     * @see normalizePoint(Point)
     * @return {@code true} if normalizing the given latitude would
     *         imply a 180&deg; shift in longitude, {@code false} otherwise.
     */
    public static boolean normalizingLatImpliesShift(double lat) {
        lat = centeredModulus(lat, 360);
        return lat < -90 || lat > 90;
    }

    /**
     * Normalize the geo {@code Point} for its coordinates to lie within their
     * respective normalized ranges.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     *       in order to normalize properly the latitude.
     *
     * @param pt The point to normalize in-place.
     */
    public static void normalizePoint(Point pt) {
        normalizePoint(pt, true, true);
    }

    /**
     * Normalize the geo {@code Point} for the given coordinates to lie within
     * their respective normalized ranges.
     *
     * You can control which coordinate gets normalized with the two flags.
     * <p>
     * Note: A shift of 180&deg; is applied in the longitude if necessary,
     *       in order to normalize properly the latitude.
     *       If normalizing latitude but not longitude, it is assumed that
     *       the longitude is in the form x+k*360, with x in ]-180;180],
     *       and k is meaningful to the application.
     *       Therefore x will be adjusted while keeping k preserved.
     *
     * @param pt The point to normalize in-place.
     * @param normLat Whether to normalize latitude or leave it as is.
     * @param normLon Whether to normalize longitude.
     */
    public static void normalizePoint(Point pt, boolean normLat, boolean normLon) {
        if (normLat) {
            pt.lat = centeredModulus(pt.lat, 360);
            boolean shift = true;
            if (pt.lat < -90) {
                pt.lat = -180 - pt.lat;
            } else if (pt.lat > 90) {
                pt.lat = 180 - pt.lat;
            } else {
                // No need to shift the longitude, and the latitude is normalized
                shift = false;
            }
            if (shift) {
                if (normLon) {
                    pt.lon += 180;
                } else {
                    // Longitude won't be normalized,
                    // keep it in the form x+k*360 (with x in ]-180;180])
                    // by only changing x, assuming k is meaningful for the user application.
                    pt.lon += normalizeLon(pt.lon) > 0 : -180 : 180;
                }
            }
        }
        if (normLon) {
            pt.lon = centeredModulus(pt.lon, 360);
        }
    }

    /**
     * Normalizes the given geo range, eventually splitting it.
     * -90 (lat) and -180 (lon) are valid lower bounds for ranges.
     * <p>
     * Does not modify invalid ranges (lower bound &gt; higher bound).
     *
     * @return A list of the normalized ranges that map the same area.
     */
    public static List<Range> normalizeRange(Range range) {
        return normalizeRange(range.topLeft, range.bottomRight);
    }

    /**
     * Normalizes the geo range defined by the two points,
     * eventually splitting it.
     * -90 (lat) and -180 (lon) are valid lower bounds for ranges.
     * <p>
     * Does not modify invalid ranges (lower bound &gt; higher bound).
     *
     * @return A list of the normalized ranges that map the same area.
     */
    public static List<Range> normalizeRange(Point topLeft, Point bottomRight) {
        List<Range> rtn;
        // Work on a copy
        topLeft = new Point(topLeft.lat, topLeft.lon);
        bottomRight = new Point(bottomRight.lat, bottomRight.lon);
        // Skip invalid ranges
        double height = topLeft.lat - bottomRight.lat;
        double width = bottomRight.lon - topLeft.lon;
        if (height < 0 || width < 0) {
            rtn = new ArrayList<Range>(1);
            rtn.add(new Range(topLeft, bottomRight));
            return rtn;
        }
        // Normalize the left longitude and either or both latitudes
        // and preserve range width/height or clamp overfull width/height.
        if (height >= 180) {
            topLeft.lat = 90;
            bottomRight.lat = -90;
        } else {
            topLeft.lat = centeredModulus(topLeft.lat, 360);
            bottomRight.lat = topLeft.lat - height;
            if (bottomRight.lat < -180) {
                // Apply the reflection normalization
                // and swap top and bottom latitude
                topLeft.lat = -360 - bottomRight.lat;
                bottomRight.lat = topLeft.lat - height;
                topLeft.lon += 180;
                bottomRight.lon += 180;
            }

            boolean topReflects = normalizingLatImpliesShift(topLeft.lat);
            boolean bottomReflects = normalizingLatImpliesShift(bottomRight.lat);
            if (topReflects == bottomReflects) {
                normalizePoint(topLeft);
                normalizePoint(bottomRight);
                // No +/- 90° lat crossing (remember, height < 180°)
            } else {
                topLeft.lat = centeredModulus(topLeft.lat, 360);
                bottomRight.lat = centeredModulus(bottomRight.lat, 360);
                // Either top or bottom latitude lies within [-90;90], not both
                // ie: either -90° lat or +90° lat crossing
            }
        }
        if (width >= 360) {
            topLeft.lon = -180;
            bottomRight.lon = 180;
        } else {
            topLeft.lon = normalizeLon(topLeft.lon);
            bottomRight.lon = topLeft.lon + width;
        }
        // Note: This leaves us with the left longitude normalized,
        //       and both top and bottom latitude lying within [-180;180],
        //       with at most one of them lying outside [-90;90] (non normalized).
        // Check for splitting and wrapping necessity
        if (bottomRight.lat < -90) {
            // We must handle reflection around the bottom edge
            if (bottomRight.lon > 180) {
                // We must handle wrapping around the right edge
                rtn = new ArrayList<Range>(3);
                rtn.add(new Range(topLeft.lat, topLeft.lon, -90, 180)); // top left part, near the bottom right corner
                rtn.add(new Range(topLeft.lat, -180, -90, bottomRight.lon-360)); // top right part, wrapped near the bottom left corner
                rtn.add(new Range(-180-bottomRight.lat, topLeft.lon-180, -90, bottomRight.lon-180)); // bottom part, reflected near the bottom center
            } else {
                // Longitude is normalized
                if (Math.signum(topLeft.lon) != Math.signum(bottomRight.lon)) {
                    // The reflected bottom part will cross the 180° lon, we have to split it
                    rtn = new ArrayList<Range>(3);
                    rtn.add(new Range(topLeft.lat, topLeft.lon, -90, bottomRight.lon)); // the top part, above the bottom center
                    rtn.add(new Range(-180-bottomRight.lat, topLeft.lon+180, -90, 180)); // the bottom left part, near the bottom right corner
                    rtn.add(new Range(-180-bottomRight.lat, -180, -90, bottomRight.lon-180)); // the bottom right part, near the bottom left corner
                } else {
                    rtn = new ArrayList<Range>(2);
                    rtn.add(new Range(topLeft.lat, topLeft.lon, -90, bottomRight.lon)); // top part, above the bottom edge
                    rtn.add(new Range(-180-bottomRight.lat, normalizeLon(topLeft.lon+180), -90, normalizeLon(bottomRight.lon+180))); // bottom part, reflected above the opposite bottom edge
                }
            }
        } else if (topLeft.lat > 90) {
            // We must handle reflection around the top edge
            if (bottomRight.lon > 180) {
                // We must handle wrapping around the right edge
                rtn = new ArrayList<Range>(3);
                rtn.add(new Range(90, topLeft.lon-180, 180-topLeft.lat, bottomRight.lon-180)); // top part, reflected near the top center
                rtn.add(new Range(90, topLeft.lon, bottomRight.lat, 180)); // bottom left part, near the top right corner
                rtn.add(new Range(90, -180, bottomRight.lat, bottomRight.lon-360)); // bottom right part, wrapped near the top left corner
            } else {
                // Longitude is normalized
                if (Math.signum(topLeft.lon) != Math.signum(bottomRight.lon)) {
                    // The reflected bottom part will cross the 180° lon, we have to split it
                    rtn = new ArrayList<Range>(3);
                    rtn.add(new Range(90, topLeft.lon, bottomRight.lat, bottomRight.lon)); // bottom part, under the top center
                    rtn.add(new Range(90, topLeft.lon+180, 180-topLeft.lat, 180)); // top left part, near the top right corner
                    rtn.add(new Range(90, -180, 180-topLeft.lat, bottomRight.lon-180)); // top right part, neat the top left corner
                } else {
                    rtn = new ArrayList<Range>(2);
                    rtn.add(new Range(90, normalizeLon(topLeft.lon+180), 180-topLeft.lat, normalizeLon(bottomRight.lon+180))); // top part, reflected under the opposite top edge
                    rtn.add(new Range(90, topLeft.lon, bottomRight.lat, bottomRight.lon)); // bottom part, under the top edge
                }
            }
        } else {
            // Latitude is normalized
            if (bottomRight.lon > 180) {
                rtn = new ArrayList<Range>(2);
                rtn.add(new Range(topLeft.lat, topLeft.lon, bottomRight.lat, 180)); // left part, at left of the right edge
                rtn.add(new Range(topLeft.lat, -180, bottomRight.lat, bottomRight.lon-360)); // right part, wrapped on the right of the left edge
            } else {
                // Longitude is normalized, too
                rtn = new ArrayList<Range>(1);
                rtn.add(new Range(topLeft.lat, topLeft.lon, bottomRight.lat, bottomRight.lon)); // intact
            }
        }
        return rtn;
    }

    private static double centeredModulus(double dividend, double divisor) {
        double rtn = dividend % divisor;
        if (rtn <= 0)
            rtn += divisor;
        if (rtn > divisor/2)
            rtn -= divisor;
        return rtn;
    }

}
