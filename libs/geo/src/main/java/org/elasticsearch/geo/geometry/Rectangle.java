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
 * Represents a lat/lon rectangle in decimal degrees and optional altitude in meters.
 */
public class Rectangle implements Geometry {
    public static final Rectangle EMPTY = new Rectangle();
    /**
     * maximum longitude value (in degrees)
     */
    private final double minLat;
    /**
     * minimum longitude value (in degrees)
     */
    private final double minLon;
    /**
     * maximum altitude value (in meters)
     */
    private final double minAlt;
    /**
     * maximum latitude value (in degrees)
     */
    private final double maxLat;
    /**
     * minimum latitude value (in degrees)
     */
    private final double maxLon;
    /**
     * minimum altitude value (in meters)
     */
    private final double maxAlt;

    private final boolean empty;

    private Rectangle() {
        minLat = 0;
        minLon = 0;
        maxLat = 0;
        maxLon = 0;
        minAlt = Double.NaN;
        maxAlt = Double.NaN;
        empty = true;
    }

    /**
     * Constructs a bounding box by first validating the provided latitude and longitude coordinates
     */
    public Rectangle(double minLat, double maxLat, double minLon, double maxLon) {
        this(minLat, maxLat, minLon, maxLon, Double.NaN, Double.NaN);
    }
    /**
     * Constructs a bounding box by first validating the provided latitude and longitude coordinates
     */
    public Rectangle(double minLat, double maxLat, double minLon, double maxLon, double minAlt, double maxAlt) {
        GeometryUtils.checkLatitude(minLat);
        GeometryUtils.checkLatitude(maxLat);
        GeometryUtils.checkLongitude(minLon);
        GeometryUtils.checkLongitude(maxLon);
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minAlt = minAlt;
        this.maxAlt = maxAlt;
        empty = false;
        if (maxLat < minLat) {
            throw new IllegalArgumentException("max lat cannot be less than min lat");
        }
        if (Double.isNaN(minAlt) != Double.isNaN(maxAlt)) {
            throw new IllegalArgumentException("only one altitude value is specified");
        }
    }

    public double getWidth() {
        if (crossesDateline()) {
            return GeometryUtils.MAX_LON_INCL - minLon + maxLon - GeometryUtils.MIN_LON_INCL;
        }
        return maxLon - minLon;
    }

    public double getHeight() {
        return maxLat - minLat;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMinLon() {
        return minLon;
    }


    public double getMinAlt() {
        return minAlt;
    }

    public double getMaxLat() {
        return maxLat;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public double getMaxAlt() {
        return maxAlt;
    }

    @Override
    public ShapeType type() {
        return ShapeType.ENVELOPE;
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
        if (hasAlt()) {
            b.append(" alt=");
            b.append(minAlt);
            b.append(" TO ");
            b.append(maxAlt);
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

    /** returns true if rectangle (defined by minLat, maxLat, minLon, maxLon) contains the lat lon point */
    public boolean containsPoint(final double lat, final double lon) {
        if (lat >= minLat && lat <= maxLat) {
            return crossesDateline() ? lon >= minLon || lon <= maxLon : lon >= minLon && lon <= maxLon;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rectangle rectangle = (Rectangle) o;

        if (Double.compare(rectangle.minLat, minLat) != 0) return false;
        if (Double.compare(rectangle.minLon, minLon) != 0) return false;
        if (Double.compare(rectangle.maxLat, maxLat) != 0) return false;
        if (Double.compare(rectangle.maxLon, maxLon) != 0) return false;
        if (Double.compare(rectangle.minAlt, minAlt) != 0) return false;
        return Double.compare(rectangle.maxAlt, maxAlt) == 0;

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
        temp = Double.doubleToLongBits(minAlt);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxAlt);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public boolean hasAlt() {
        return Double.isNaN(maxAlt) == false;
    }
}
