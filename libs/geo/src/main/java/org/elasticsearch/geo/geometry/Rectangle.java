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

import java.io.IOException;

import org.elasticsearch.geo.GeoUtils;
import org.elasticsearch.geo.parsers.WKTParser;
import org.apache.lucene.store.OutputStreamDataOutput;

import static java.lang.Math.PI;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.geo.GeoUtils.checkLatitude;
import static org.elasticsearch.geo.GeoUtils.checkLongitude;
import static org.elasticsearch.geo.GeoUtils.MAX_LAT_INCL;
import static org.elasticsearch.geo.GeoUtils.MIN_LAT_INCL;
import static org.elasticsearch.geo.GeoUtils.MAX_LAT_RADIANS;
import static org.elasticsearch.geo.GeoUtils.MAX_LON_RADIANS;
import static org.elasticsearch.geo.GeoUtils.MIN_LAT_RADIANS;
import static org.elasticsearch.geo.GeoUtils.MIN_LON_RADIANS;
import static org.elasticsearch.geo.GeoUtils.EARTH_MEAN_RADIUS_METERS;
import static org.elasticsearch.geo.GeoUtils.sloppySin;
import static org.apache.lucene.util.SloppyMath.TO_DEGREES;
import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.toDegrees;
import static org.apache.lucene.util.SloppyMath.toRadians;

/**
 * Represents a lat/lon rectangle in decimal degrees.
 */
public class Rectangle extends GeoShape {
    /**
     * maximum longitude value (in degrees)
     */
    public final double minLat;
    /**
     * minimum longitude value (in degrees)
     */
    public final double minLon;
    /**
     * maximum latitude value (in degrees)
     */
    public final double maxLat;
    /**
     * minimum latitude value (in degrees)
     */
    public final double maxLon;
    /**
     * center of rectangle (in lat/lon degrees)
     */
    private final Point center;

    /**
     * Constructs a bounding box by first validating the provided latitude and longitude coordinates
     */
    public Rectangle(double minLat, double maxLat, double minLon, double maxLon) {
        GeoUtils.checkLatitude(minLat);
        GeoUtils.checkLatitude(maxLat);
        GeoUtils.checkLongitude(minLon);
        GeoUtils.checkLongitude(maxLon);
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        assert maxLat >= minLat;

        // NOTE: cannot assert maxLon >= minLon since this rect could cross the dateline
        this.boundingBox = this;

        // compute the center of the rectangle
        final double cntrLat = getHeight() / 2 + minLat;
        double cntrLon = getWidth() / 2 + minLon;
        if (crossesDateline()) {
            cntrLon = GeoUtils.normalizeLonDegrees(cntrLon);
        }
        this.center = new Point(cntrLat, cntrLon);
    }

    @Override
    public boolean hasArea() {
        return minLat != maxLat && minLon != maxLon;
    }

    public double getWidth() {
        if (crossesDateline()) {
            return GeoUtils.MAX_LON_INCL - minLon + maxLon - GeoUtils.MIN_LON_INCL;
        }
        return maxLon - minLon;
    }

    public double getHeight() {
        return maxLat - minLat;
    }

    public Point getCenter() {
        return this.center;
    }

    @Override
    public ShapeType type() {
        return ShapeType.ENVELOPE;
    }

    @Override
    public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
        if (minLat == GeoUtils.MIN_LAT_INCL && maxLat == GeoUtils.MAX_LAT_INCL
            && minLon == GeoUtils.MIN_LON_INCL && maxLon == GeoUtils.MAX_LON_INCL) {
            return Relation.WITHIN;
        } else if (this.minLat == GeoUtils.MIN_LAT_INCL && this.maxLat == GeoUtils.MAX_LAT_INCL
            && this.minLon == GeoUtils.MIN_LON_INCL && this.maxLon == GeoUtils.MAX_LON_INCL) {
            return Relation.CONTAINS;
        } else if (crossesDateline() == true) {
            return relateXDL(minLat, maxLat, minLon, maxLon);
        } else if (minLon > maxLon) {
            if (rectDisjoint(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, minLon, GeoUtils.MAX_LON_INCL)
                && rectDisjoint(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, GeoUtils.MIN_LON_INCL, maxLon)) {
                return Relation.DISJOINT;
            } else if (rectWithin(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, minLon, GeoUtils.MAX_LON_INCL)
                || rectWithin(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, GeoUtils.MIN_LON_INCL, maxLon)) {
                return Relation.WITHIN;
            } // can't contain
        } else if (rectDisjoint(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, minLon, maxLon)) {
            return Relation.DISJOINT;
        } else if (rectWithin(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, minLon, maxLon)) {
            return Relation.WITHIN;
        } else if (rectWithin(minLat, maxLat, minLon, maxLon, this.minLat, this.maxLat, this.minLon, this.maxLon)) {
            return Relation.CONTAINS;
        }
        return Relation.INTERSECTS;
    }

    /**
     * compute relation for this Rectangle crossing the dateline
     */
    private Relation relateXDL(double minLat, double maxLat, double minLon, double maxLon) {
        if (minLon > maxLon) {
            // incoming rectangle crosses dateline; just check latitude for disjoint
            if (this.minLat > maxLat || this.maxLat < minLat) {
                return Relation.DISJOINT;
            } else if (rectWithin(this.minLat, this.maxLat, this.minLon, this.maxLon, minLat, maxLat, minLon, maxLon)) {
                return Relation.WITHIN;
            } else if (rectWithin(minLat, maxLat, minLon, maxLon, this.minLat, this.maxLat, this.minLon, this.maxLon)) {
                return Relation.CONTAINS;
            }
        } else {
            if (rectDisjoint(this.minLat, this.maxLat, this.minLon, GeoUtils.MAX_LON_INCL, minLat, maxLat, minLon, maxLon)
                && rectDisjoint(this.minLat, this.maxLat, GeoUtils.MIN_LON_INCL, this.maxLon, minLat, maxLat, minLon, maxLon)) {
                return Relation.DISJOINT;
                // WITHIN not possible; *this* rectangle crosses the dateline but *that* rectangle does not
            } else if (rectWithin(minLat, maxLat, minLon, maxLon, this.minLat, this.maxLat, this.minLon, GeoUtils.MAX_LON_INCL)
                || rectWithin(minLat, maxLat, minLon, maxLon, this.minLat, this.maxLat, GeoUtils.MIN_LON_INCL, this.maxLon)) {
                return Relation.CONTAINS;
            }
        }
        return Relation.INTERSECTS;
    }

    public Relation relate(double lat, double lon) {
        if (lat > maxLat || lat < minLat || lon < minLon || lon > maxLon) {
            return Relation.DISJOINT;
        }
        return Relation.INTERSECTS;
    }

    @Override
    public Relation relate(GeoShape shape) {
        Relation r = shape.relate(this.minLat, this.maxLat, this.minLon, this.maxLon);
        if (r == Relation.WITHIN) {
            return Relation.CONTAINS;
        } else if (r == Relation.CONTAINS) {
            return Relation.WITHIN;
        }
        return r;
    }

    /**
     * Computes whether two rectangles are disjoint
     */
    private static boolean rectDisjoint(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                        final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
        assert aMinLon <= aMaxLon : "dateline crossing not supported";
        assert bMinLon <= bMaxLon : "dateline crossing not supported";
        // fail quickly: test latitude
        if (aMaxLat < bMinLat || aMinLat > bMaxLat) {
            return true;
        }

        // check sharing dateline
        if ((aMinLon == GeoUtils.MIN_LON_INCL && bMaxLon == GeoUtils.MAX_LON_INCL)
            || (bMinLon == GeoUtils.MIN_LON_INCL && aMaxLon == GeoUtils.MAX_LON_INCL)) {
            return false;
        }

        // check longitude
        return aMaxLon < bMinLon || aMinLon > bMaxLon;
    }

    /**
     * Computes whether the first (a) rectangle is wholly within another (b) rectangle (shared boundaries allowed)
     */
    private static boolean rectWithin(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                      final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
        return !(aMinLon < bMinLon || aMinLat < bMinLat || aMaxLon > bMaxLon || aMaxLat > bMaxLat);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Rectangle(lat=");
        b.append(minLat);
        b.append(" TO ");
        b.append(maxLat);
        b.append(" lon=");
        b.append(minLon);
        b.append(" TO ");
        b.append(maxLon);
        if (maxLon < minLon) {
            b.append(" [crosses dateline!]");
        }
        b.append(")");

        return b.toString();
    }

    /**
     * Returns true if this bounding box crosses the dateline
     */
    public boolean crossesDateline() {
        return maxLon < minLon;
    }

    /**
     * Compute Bounding Box for a circle using WGS-84 parameters
     */
    public static Rectangle fromPointDistance(final double centerLat, final double centerLon, final double radiusMeters) {
        checkLatitude(centerLat);
        checkLongitude(centerLon);
        final double radLat = toRadians(centerLat);
        final double radLon = toRadians(centerLon);
        // LUCENE-7143
        double radDistance = (radiusMeters + 7E-2) / EARTH_MEAN_RADIUS_METERS;
        double minLat = radLat - radDistance;
        double maxLat = radLat + radDistance;
        double minLon;
        double maxLon;

        if (minLat > MIN_LAT_RADIANS && maxLat < MAX_LAT_RADIANS) {
            double deltaLon = asin(sloppySin(radDistance) / cos(radLat));
            minLon = radLon - deltaLon;
            if (minLon < MIN_LON_RADIANS) {
                minLon += 2d * PI;
            }
            maxLon = radLon + deltaLon;
            if (maxLon > MAX_LON_RADIANS) {
                maxLon -= 2d * PI;
            }
        } else {
            // a pole is within the distance
            minLat = max(minLat, MIN_LAT_RADIANS);
            maxLat = min(maxLat, MAX_LAT_RADIANS);
            minLon = MIN_LON_RADIANS;
            maxLon = MAX_LON_RADIANS;
        }

        return new Rectangle(toDegrees(minLat), toDegrees(maxLat), toDegrees(minLon), toDegrees(maxLon));
    }

    /**
     * maximum error from {@link #axisLat(double, double)}. logic must be prepared to handle this
     */
    public static final double AXISLAT_ERROR = 0.1D / EARTH_MEAN_RADIUS_METERS * TO_DEGREES;

    /**
     * Calculate the latitude of a circle's intersections with its bbox meridians.
     * <p>
     * <b>NOTE:</b> the returned value will be +/- {@link #AXISLAT_ERROR} of the actual value.
     *
     * @param centerLat    The latitude of the circle center
     * @param radiusMeters The radius of the circle in meters
     * @return A latitude
     */
    public static double axisLat(double centerLat, double radiusMeters) {
        // A spherical triangle with:
        // r is the radius of the circle in radians
        // l1 is the latitude of the circle center
        // l2 is the latitude of the point at which the circle intersect's its bbox longitudes
        // We know r is tangent to the bbox meridians at l2, therefore it is a right angle.
        // So from the law of cosines, with the angle of l1 being 90, we have:
        // cos(l1) = cos(r) * cos(l2) + sin(r) * sin(l2) * cos(90)
        // The second part cancels out because cos(90) == 0, so we have:
        // cos(l1) = cos(r) * cos(l2)
        // Solving for l2, we get:
        // l2 = acos( cos(l1) / cos(r) )
        // We ensure r is in the range (0, PI/2) and l1 in the range (0, PI/2]. This means we
        // cannot divide by 0, and we will always get a positive value in the range [0, 1) as
        // the argument to arc cosine, resulting in a range (0, PI/2].
        final double PIO2 = Math.PI / 2D;
        double l1 = toRadians(centerLat);
        double r = (radiusMeters + 7E-2) / EARTH_MEAN_RADIUS_METERS;

        // if we are within radius range of a pole, the lat is the pole itself
        if (Math.abs(l1) + r >= MAX_LAT_RADIANS) {
            return centerLat >= 0 ? MAX_LAT_INCL : MIN_LAT_INCL;
        }

        // adjust l1 as distance from closest pole, to form a right triangle with bbox meridians
        // and ensure it is in the range (0, PI/2]
        l1 = centerLat >= 0 ? PIO2 - l1 : l1 + PIO2;

        double l2 = Math.acos(Math.cos(l1) / Math.cos(r));
        assert !Double.isNaN(l2);

        // now adjust back to range [-pi/2, pi/2], ie latitude in radians
        l2 = centerLat >= 0 ? PIO2 - l2 : l2 - PIO2;

        return toDegrees(l2);
    }

    /**
     * Returns the bounding box over an array of polygons
     */
    public static Rectangle fromPolygon(Polygon[] polygons) {
        // compute bounding box
        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < polygons.length; i++) {
            minLat = min(polygons[i].minLat(), minLat);
            maxLat = max(polygons[i].maxLat(), maxLat);
            minLon = min(polygons[i].minLon(), minLon);
            maxLon = max(polygons[i].maxLon(), maxLon);
        }

        return new Rectangle(minLat, maxLat, minLon, maxLon);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rectangle rectangle = (Rectangle) o;

        if (Double.compare(rectangle.minLat, minLat) != 0) return false;
        if (Double.compare(rectangle.minLon, minLon) != 0) return false;
        if (Double.compare(rectangle.maxLat, maxLat) != 0) return false;
        return Double.compare(rectangle.maxLon, maxLon) == 0;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(minLat);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minLon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxLat);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxLon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    protected StringBuilder contentToWKT() {
        StringBuilder sb = new StringBuilder();

        sb.append(WKTParser.LPAREN);
        // minX, maxX, maxY, minY
        sb.append(minLon);
        sb.append(WKTParser.COMMA);
        sb.append(WKTParser.SPACE);
        sb.append(maxLon);
        sb.append(WKTParser.COMMA);
        sb.append(WKTParser.SPACE);
        sb.append(maxLat);
        sb.append(WKTParser.COMMA);
        sb.append(WKTParser.SPACE);
        sb.append(minLat);
        sb.append(WKTParser.RPAREN);

        return sb;
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        out.writeVLong(Double.doubleToRawLongBits(minLon));
        out.writeVLong(Double.doubleToRawLongBits(maxLon));
        out.writeVLong(Double.doubleToRawLongBits(maxLat));
        out.writeVLong(Double.doubleToRawLongBits(minLat));
    }
}
