package org.apache.lucene.util;

/*
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

import java.util.ArrayList;

/**
 * Basic reusable geo-spatial utility methods
 *
 * @lucene.experimental
 */
public final class XGeoUtils {
    private static final short MIN_LON = -180;
    private static final short MIN_LAT = -90;
    public static final short BITS = 31;
    private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
    private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
    public static final double TOLERANCE = 1E-6;

    /** Minimum longitude value. */
    public static final double MIN_LON_INCL = -180.0D;

    /** Maximum longitude value. */
    public static final double MAX_LON_INCL = 180.0D;

    /** Minimum latitude value. */
    public static final double MIN_LAT_INCL = -90.0D;

    /** Maximum latitude value. */
    public static final double MAX_LAT_INCL = 90.0D;

    // magic numbers for bit interleaving
    private static final long MAGIC[] = {
            0x5555555555555555L, 0x3333333333333333L,
            0x0F0F0F0F0F0F0F0FL, 0x00FF00FF00FF00FFL,
            0x0000FFFF0000FFFFL, 0x00000000FFFFFFFFL,
            0xAAAAAAAAAAAAAAAAL
    };
    // shift values for bit interleaving
    private static final short SHIFT[] = {1, 2, 4, 8, 16};

    public static double LOG2 = StrictMath.log(2);

    // No instance:
    private XGeoUtils() {
    }

    public static Long mortonHash(final double lon, final double lat) {
        return interleave(scaleLon(lon), scaleLat(lat));
    }

    public static double mortonUnhashLon(final long hash) {
        return unscaleLon(deinterleave(hash));
    }

    public static double mortonUnhashLat(final long hash) {
        return unscaleLat(deinterleave(hash >>> 1));
    }

    private static long scaleLon(final double val) {
        return (long) ((val-MIN_LON) * LON_SCALE);
    }

    private static long scaleLat(final double val) {
        return (long) ((val-MIN_LAT) * LAT_SCALE);
    }

    private static double unscaleLon(final long val) {
        return (val / LON_SCALE) + MIN_LON;
    }

    private static double unscaleLat(final long val) {
        return (val / LAT_SCALE) + MIN_LAT;
    }

    /**
     * Interleaves the first 32 bits of each long value
     *
     * Adapted from: http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
     */
    public static long interleave(long v1, long v2) {
        v1 = (v1 | (v1 << SHIFT[4])) & MAGIC[4];
        v1 = (v1 | (v1 << SHIFT[3])) & MAGIC[3];
        v1 = (v1 | (v1 << SHIFT[2])) & MAGIC[2];
        v1 = (v1 | (v1 << SHIFT[1])) & MAGIC[1];
        v1 = (v1 | (v1 << SHIFT[0])) & MAGIC[0];
        v2 = (v2 | (v2 << SHIFT[4])) & MAGIC[4];
        v2 = (v2 | (v2 << SHIFT[3])) & MAGIC[3];
        v2 = (v2 | (v2 << SHIFT[2])) & MAGIC[2];
        v2 = (v2 | (v2 << SHIFT[1])) & MAGIC[1];
        v2 = (v2 | (v2 << SHIFT[0])) & MAGIC[0];

        return (v2<<1) | v1;
    }

    /**
     * Deinterleaves long value back to two concatenated 32bit values
     */
    public static long deinterleave(long b) {
        b &= MAGIC[0];
        b = (b ^ (b >>> SHIFT[0])) & MAGIC[1];
        b = (b ^ (b >>> SHIFT[1])) & MAGIC[2];
        b = (b ^ (b >>> SHIFT[2])) & MAGIC[3];
        b = (b ^ (b >>> SHIFT[3])) & MAGIC[4];
        b = (b ^ (b >>> SHIFT[4])) & MAGIC[5];
        return b;
    }

    public static double compare(final double v1, final double v2) {
        final double compare = v1-v2;
        return Math.abs(compare) <= TOLERANCE ? 0 : compare;
    }

    /**
     * Puts longitude in range of -180 to +180.
     */
    public static double normalizeLon(double lon_deg) {
        if (lon_deg >= -180 && lon_deg <= 180) {
            return lon_deg; //common case, and avoids slight double precision shifting
        }
        double off = (lon_deg + 180) % 360;
        if (off < 0) {
            return 180 + off;
        } else if (off == 0 && lon_deg > 0) {
            return 180;
        } else {
            return -180 + off;
        }
    }

    /**
     * Puts latitude in range of -90 to 90.
     */
    public static double normalizeLat(double lat_deg) {
        if (lat_deg >= -90 && lat_deg <= 90) {
            return lat_deg; //common case, and avoids slight double precision shifting
        }
        double off = Math.abs((lat_deg + 90) % 360);
        return (off <= 180 ? off : 360-off) - 90;
    }

    public static final boolean bboxContains(final double lon, final double lat, final double minLon,
                                             final double minLat, final double maxLon, final double maxLat) {
        return (compare(lon, minLon) >= 0 && compare(lon, maxLon) <= 0
                && compare(lat, minLat) >= 0 && compare(lat, maxLat) <= 0);
    }

    /**
     * simple even-odd point in polygon computation
     *    1.  Determine if point is contained in the longitudinal range
     *    2.  Determine whether point crosses the edge by computing the latitudinal delta
     *        between the end-point of a parallel vector (originating at the point) and the
     *        y-component of the edge sink
     *
     * NOTE: Requires polygon point (x,y) order either clockwise or counter-clockwise
     */
    public static boolean pointInPolygon(double[] x, double[] y, double lat, double lon) {
        assert x.length == y.length;
        boolean inPoly = false;
        /**
         * Note: This is using a euclidean coordinate system which could result in
         * upwards of 110KM error at the equator.
         * TODO convert coordinates to cylindrical projection (e.g. mercator)
         */
        for (int i = 1; i < x.length; i++) {
            if (x[i] < lon && x[i-1] >= lon || x[i-1] < lon && x[i] >= lon) {
                if (y[i] + (lon - x[i]) / (x[i-1] - x[i]) * (y[i-1] - y[i]) < lat) {
                    inPoly = !inPoly;
                }
            }
        }
        return inPoly;
    }

    public static String geoTermToString(long term) {
        StringBuilder s = new StringBuilder(64);
        final int numberOfLeadingZeros = Long.numberOfLeadingZeros(term);
        for (int i = 0; i < numberOfLeadingZeros; i++) {
            s.append('0');
        }
        if (term != 0) {
            s.append(Long.toBinaryString(term));
        }
        return s.toString();
    }


    public static boolean rectDisjoint(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                       final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
        return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
    }

    /**
     * Computes whether a rectangle is wholly within another rectangle (shared boundaries allowed)
     */
    public static boolean rectWithin(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                     final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
        return !(aMinX < bMinX || aMinY < bMinY || aMaxX > bMaxX || aMaxY > bMaxY);
    }

    public static boolean rectCrosses(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                      final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
        return !(rectDisjoint(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY) ||
                rectWithin(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY));
    }

    /**
     * Computes whether rectangle a contains rectangle b (touching allowed)
     */
    public static boolean rectContains(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                       final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
        return !(bMinX < aMinX || bMinY < aMinY || bMaxX > aMaxX || bMaxY > aMaxY);
    }

    /**
     * Computes whether a rectangle intersects another rectangle (crosses, within, touching, etc)
     */
    public static boolean rectIntersects(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                         final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
        return !((aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY) );
    }

    /**
     * Computes whether a rectangle crosses a shape. (touching not allowed)
     */
    public static boolean rectCrossesPoly(final double rMinX, final double rMinY, final double rMaxX,
                                          final double rMaxY, final double[] shapeX, final double[] shapeY,
                                          final double sMinX, final double sMinY, final double sMaxX,
                                          final double sMaxY) {
        // short-circuit: if the bounding boxes are disjoint then the shape does not cross
        if (rectDisjoint(rMinX, rMinY, rMaxX, rMaxY, sMinX, sMinY, sMaxX, sMaxY)) {
            return false;
        }

        final double[][] bbox = new double[][] { {rMinX, rMinY}, {rMaxX, rMinY}, {rMaxX, rMaxY}, {rMinX, rMaxY}, {rMinX, rMinY} };
        final int polyLength = shapeX.length-1;
        double d, s, t, a1, b1, c1, a2, b2, c2;
        double x00, y00, x01, y01, x10, y10, x11, y11;

        // computes the intersection point between each bbox edge and the polygon edge
        for (short b=0; b<4; ++b) {
            a1 = bbox[b+1][1]-bbox[b][1];
            b1 = bbox[b][0]-bbox[b+1][0];
            c1 = a1*bbox[b+1][0] + b1*bbox[b+1][1];
            for (int p=0; p<polyLength; ++p) {
                a2 = shapeY[p+1]-shapeY[p];
                b2 = shapeX[p]-shapeX[p+1];
                // compute determinant
                d = a1*b2 - a2*b1;
                if (d != 0) {
                    // lines are not parallel, check intersecting points
                    c2 = a2*shapeX[p+1] + b2*shapeY[p+1];
                    s = (1/d)*(b2*c1 - b1*c2);
                    t = (1/d)*(a1*c2 - a2*c1);
                    x00 = StrictMath.min(bbox[b][0], bbox[b+1][0]) - TOLERANCE;
                    x01 = StrictMath.max(bbox[b][0], bbox[b+1][0]) + TOLERANCE;
                    y00 = StrictMath.min(bbox[b][1], bbox[b+1][1]) - TOLERANCE;
                    y01 = StrictMath.max(bbox[b][1], bbox[b+1][1]) + TOLERANCE;
                    x10 = StrictMath.min(shapeX[p], shapeX[p+1]) - TOLERANCE;
                    x11 = StrictMath.max(shapeX[p], shapeX[p+1]) + TOLERANCE;
                    y10 = StrictMath.min(shapeY[p], shapeY[p+1]) - TOLERANCE;
                    y11 = StrictMath.max(shapeY[p], shapeY[p+1]) + TOLERANCE;
                    // check whether the intersection point is touching one of the line segments
                    boolean touching = ((x00 == s && y00 == t) || (x01 == s && y01 == t))
                            || ((x10 == s && y10 == t) || (x11 == s && y11 == t));
                    // if line segments are not touching and the intersection point is within the range of either segment
                    if (!(touching || x00 > s || x01 < s || y00 > t || y01 < t || x10 > s || x11 < s || y10 > t || y11 < t)) {
                        return true;
                    }
                }
            } // for each poly edge
        } // for each bbox edge
        return false;
    }

    /**
     * Converts a given circle (defined as a point/radius) to an approximated line-segment polygon
     *
     * @param lon longitudinal center of circle (in degrees)
     * @param lat latitudinal center of circle (in degrees)
     * @param radius distance radius of circle (in meters)
     * @return a list of lon/lat points representing the circle
     */
    @SuppressWarnings({"unchecked","rawtypes"})
    public static ArrayList<double[]> circleToPoly(final double lon, final double lat, final double radius) {
        double angle;
        // a little under-sampling (to limit the number of polygonal points): using archimedes estimation of pi
        final int sides = 25;
        ArrayList<double[]> geometry = new ArrayList();
        double[] lons = new double[sides];
        double[] lats = new double[sides];

        double[] pt = new double[2];
        final int sidesLen = sides-1;
        for (int i=0; i<sidesLen; ++i) {
            angle = (i*360/sides);
            pt = XGeoProjectionUtils.pointFromLonLatBearing(lon, lat, angle, radius, pt);
            lons[i] = pt[0];
            lats[i] = pt[1];
        }
        // close the poly
        lons[sidesLen] = lons[0];
        lats[sidesLen] = lats[0];
        geometry.add(lons);
        geometry.add(lats);

        return geometry;
    }

    /**
     * Computes whether a rectangle is within a given polygon (shared boundaries allowed)
     */
    public static boolean rectWithinPoly(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                         final double[] shapeX, final double[] shapeY, final double sMinX,
                                         final double sMinY, final double sMaxX, final double sMaxY) {
        // check if rectangle crosses poly (to handle concave/pacman polys), then check that all 4 corners
        // are contained
        return !(rectCrossesPoly(rMinX, rMinY, rMaxX, rMaxY, shapeX, shapeY, sMinX, sMinY, sMaxX, sMaxY) ||
                !pointInPolygon(shapeX, shapeY, rMinY, rMinX) || !pointInPolygon(shapeX, shapeY, rMinY, rMaxX) ||
                !pointInPolygon(shapeX, shapeY, rMaxY, rMaxX) || !pointInPolygon(shapeX, shapeY, rMaxY, rMinX));
    }

    private static boolean rectAnyCornersOutsideCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                                       final double centerLon, final double centerLat, final double radius) {
        return (SloppyMath.haversin(centerLat, centerLon, rMinY, rMinX)*1000.0 > radius
                || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMinX)*1000.0 > radius
                || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMaxX)*1000.0 > radius
                || SloppyMath.haversin(centerLat, centerLon, rMinY, rMaxX)*1000.0 > radius);
    }

    private static boolean rectAnyCornersInCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                                  final double centerLon, final double centerLat, final double radius) {
        return (SloppyMath.haversin(centerLat, centerLon, rMinY, rMinX)*1000.0 <= radius
                || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMinX)*1000.0 <= radius
                || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMaxX)*1000.0 <= radius
                || SloppyMath.haversin(centerLat, centerLon, rMinY, rMaxX)*1000.0 <= radius);
    }

    public static boolean rectWithinCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                           final double centerLon, final double centerLat, final double radius) {
        return !(rectAnyCornersOutsideCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radius));
    }

    /**
     * Computes whether a rectangle crosses a circle
     */
    public static boolean rectCrossesCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                            final double centerLon, final double centerLat, final double radius) {
        return rectAnyCornersInCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radius)
                || lineCrossesSphere(rMinX, rMinY, 0, rMaxX, rMinY, 0, centerLon, centerLat, 0, radius)
                || lineCrossesSphere(rMaxX, rMinY, 0, rMaxX, rMaxY, 0, centerLon, centerLat, 0, radius)
                || lineCrossesSphere(rMaxX, rMaxY, 0, rMinX, rMaxY, 0, centerLon, centerLat, 0, radius)
                || lineCrossesSphere(rMinX, rMaxY, 0, rMinX, rMinY, 0, centerLon, centerLat, 0, radius);
    }

    /**
     * Computes whether or a 3dimensional line segment intersects or crosses a sphere
     *
     * @param lon1 longitudinal location of the line segment start point (in degrees)
     * @param lat1 latitudinal location of the line segment start point (in degrees)
     * @param alt1 altitude of the line segment start point (in degrees)
     * @param lon2 longitudinal location of the line segment end point (in degrees)
     * @param lat2 latitudinal location of the line segment end point (in degrees)
     * @param alt2 altitude of the line segment end point (in degrees)
     * @param centerLon longitudinal location of center search point (in degrees)
     * @param centerLat latitudinal location of center search point (in degrees)
     * @param centerAlt altitude of the center point (in meters)
     * @param radius search sphere radius (in meters)
     * @return whether the provided line segment is a secant of the
     */
    private static boolean lineCrossesSphere(double lon1, double lat1, double alt1, double lon2,
                                             double lat2, double alt2, double centerLon, double centerLat,
                                             double centerAlt, double radius) {
        // convert to cartesian 3d (in meters)
        double[] ecf1 = XGeoProjectionUtils.llaToECF(lon1, lat1, alt1, null);
        double[] ecf2 = XGeoProjectionUtils.llaToECF(lon2, lat2, alt2, null);
        double[] cntr = XGeoProjectionUtils.llaToECF(centerLon, centerLat, centerAlt, null);

        final double dX = ecf2[0] - ecf1[0];
        final double dY = ecf2[1] - ecf1[1];
        final double dZ = ecf2[2] - ecf1[2];
        final double fX = ecf1[0] - cntr[0];
        final double fY = ecf1[1] - cntr[1];
        final double fZ = ecf1[2] - cntr[2];

        final double a = dX*dX + dY*dY + dZ*dZ;
        final double b = 2 * (fX*dX + fY*dY + fZ*dZ);
        final double c = (fX*fX + fY*fY + fZ*fZ) - (radius*radius);

        double discrim = (b*b)-(4*a*c);
        if (discrim < 0) {
            return false;
        }

        discrim = StrictMath.sqrt(discrim);
        final double a2 = 2*a;
        final double t1 = (-b - discrim)/a2;
        final double t2 = (-b + discrim)/a2;

        if ( (t1 < 0 || t1 > 1) ) {
            return !(t2 < 0 || t2 > 1);
        }

        return true;
    }

    public static boolean isValidLat(double lat) {
        return Double.isNaN(lat) == false && lat >= MIN_LAT_INCL && lat <= MAX_LAT_INCL;
    }

    public static boolean isValidLon(double lon) {
        return Double.isNaN(lon) == false && lon >= MIN_LON_INCL && lon <= MAX_LON_INCL;
    }
}