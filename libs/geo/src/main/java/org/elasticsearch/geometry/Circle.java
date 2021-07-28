/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geometry.utils.WellKnownText;

/**
 * Circle geometry (not part of WKT standard, but used in elasticsearch) defined by lat/lon coordinates of the center in degrees
 * and optional altitude in meters.
 */
public class Circle implements Geometry {
    public static final Circle EMPTY = new Circle();
    private final double y;
    private final double x;
    private final double z;
    private final double radiusMeters;

    private Circle() {
        y = 0;
        x = 0;
        z = Double.NaN;
        radiusMeters = -1;
    }

    public Circle(final double x, final double y, final double radiusMeters) {
        this(x, y, Double.NaN, radiusMeters);
    }

    public Circle(final double x, final double y, final double z, final double radiusMeters) {
        this.y = y;
        this.x = x;
        this.radiusMeters = radiusMeters;
        this.z = z;
        if (radiusMeters < 0 ) {
            throw new IllegalArgumentException("Circle radius [" + radiusMeters + "] cannot be negative");
        }
    }

    @Override
    public ShapeType type() {
        return ShapeType.CIRCLE;
    }

    public double getY() {
        return y;
    }

    public double getX() {
        return x;
    }

    public double getRadiusMeters() {
        return radiusMeters;
    }

    public double getZ() {
        return z;
    }

    public double getLat() {
        return y;
    }

    public double getLon() {
        return x;
    }

    public double getAlt() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Circle circle = (Circle) o;
        if (Double.compare(circle.y, y) != 0) return false;
        if (Double.compare(circle.x, x) != 0) return false;
        if (Double.compare(circle.radiusMeters, radiusMeters) != 0) return false;
        return (Double.compare(circle.z, z) == 0);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radiusMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
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
        return WellKnownText.toWKT(this);
    }

    @Override
    public boolean hasZ() {
        return Double.isNaN(z) == false;
    }
}
