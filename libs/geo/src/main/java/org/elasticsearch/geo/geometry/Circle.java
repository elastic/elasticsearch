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
 * Circle geometry (not part of WKT standard, but used in elasticsearch) defined by lat/lon coordinates of the center in degrees
 * and optional altitude in meters.
 */
public class Circle implements Geometry {
    public static final Circle EMPTY = new Circle();
    private final double lat;
    private final double lon;
    private final double alt;
    private final double radiusMeters;

    private Circle() {
        lat = 0;
        lon = 0;
        alt = Double.NaN;
        radiusMeters = -1;
    }

    public Circle(final double lat, final double lon, final double radiusMeters) {
        this(lat, lon, Double.NaN, radiusMeters);
    }

    public Circle(final double lat, final double lon, final double alt, final double radiusMeters) {
        this.lat = lat;
        this.lon = lon;
        this.radiusMeters = radiusMeters;
        this.alt = alt;
        if (radiusMeters < 0 ) {
            throw new IllegalArgumentException("Circle radius [" + radiusMeters + "] cannot be negative");
        }
        GeometryUtils.checkLatitude(lat);
        GeometryUtils.checkLongitude(lon);
    }

    @Override
    public ShapeType type() {
        return ShapeType.CIRCLE;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public double getRadiusMeters() {
        return radiusMeters;
    }

    public double getAlt() {
        return alt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Circle circle = (Circle) o;
        if (Double.compare(circle.lat, lat) != 0) return false;
        if (Double.compare(circle.lon, lon) != 0) return false;
        if (Double.compare(circle.radiusMeters, radiusMeters) != 0) return false;
        return (Double.compare(circle.alt, alt) == 0);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(lat);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radiusMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(alt);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return radiusMeters < 0;
    }

    @Override
    public String toString() {
        return "lat=" + lat + ", lon=" + lon + ", radius=" + radiusMeters + (Double.isNaN(alt) ? ", alt=" + alt : "");
    }

    @Override
    public boolean hasAlt() {
        return Double.isNaN(alt) == false;
    }
}
