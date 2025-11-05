/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geometry.utils.WellKnownText;

/**
 * Represents a geographic point on the earth's surface in decimal degrees with optional altitude.
 *
 * <p>A Point is defined by x (longitude), y (latitude), and optionally z (altitude in meters).
 * Points can be empty, representing a null geometry.
 *
 * <p><b>Coordinate System:</b></p>
 * <ul>
 *   <li>x (longitude): -180 to 180 degrees</li>
 *   <li>y (latitude): -90 to 90 degrees</li>
 *   <li>z (altitude): meters above/below sea level (optional)</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a 2D point (longitude, latitude)
 * Point london = new Point(-0.1278, 51.5074);
 *
 * // Create a 3D point with altitude
 * Point mountEverest = new Point(86.9250, 27.9881, 8848.86);
 *
 * // Access coordinates
 * double lat = london.getLat();
 * double lon = london.getLon();
 * double alt = mountEverest.getAlt();
 *
 * // Use empty point
 * Point empty = Point.EMPTY;
 * }</pre>
 */
public class Point implements Geometry {
    /** A singleton empty Point instance. */
    public static final Point EMPTY = new Point();

    private final double y;
    private final double x;
    private final double z;
    private final boolean empty;

    /**
     * Constructs an empty Point.
     */
    private Point() {
        y = 0;
        x = 0;
        z = Double.NaN;
        empty = true;
    }

    /**
     * Constructs a 2D Point with the specified longitude and latitude.
     *
     * @param x the longitude in decimal degrees
     * @param y the latitude in decimal degrees
     */
    public Point(double x, double y) {
        this(x, y, Double.NaN);
    }

    /**
     * Constructs a 3D Point with the specified longitude, latitude, and altitude.
     *
     * @param x the longitude in decimal degrees
     * @param y the latitude in decimal degrees
     * @param z the altitude in meters (use Double.NaN for 2D points)
     */
    public Point(double x, double y, double z) {
        this.y = y;
        this.x = x;
        this.z = z;
        this.empty = false;
    }

    @Override
    public ShapeType type() {
        return ShapeType.POINT;
    }

    /**
     * Returns the y-coordinate (latitude) of this point.
     *
     * @return the latitude in decimal degrees
     */
    public double getY() {
        return y;
    }

    /**
     * Returns the x-coordinate (longitude) of this point.
     *
     * @return the longitude in decimal degrees
     */
    public double getX() {
        return x;
    }

    /**
     * Returns the z-coordinate (altitude) of this point.
     *
     * @return the altitude in meters, or Double.NaN if not set
     */
    public double getZ() {
        return z;
    }

    /**
     * Returns the latitude of this point.
     *
     * <p>This is an alias for {@link #getY()}.
     *
     * @return the latitude in decimal degrees
     */
    public double getLat() {
        return y;
    }

    /**
     * Returns the longitude of this point.
     *
     * <p>This is an alias for {@link #getX()}.
     *
     * @return the longitude in decimal degrees
     */
    public double getLon() {
        return x;
    }

    /**
     * Returns the altitude of this point.
     *
     * <p>This is an alias for {@link #getZ()}.
     *
     * @return the altitude in meters, or Double.NaN if not set
     */
    public double getAlt() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point point = (Point) o;
        if (point.empty != empty) return false;
        if (Double.compare(point.y, y) != 0) return false;
        if (Double.compare(point.x, x) != 0) return false;
        return Double.compare(point.z, z) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
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
        return empty;
    }

    @Override
    public boolean hasZ() {
        return Double.isNaN(z) == false;
    }

    @Override
    public String toString() {
        return WellKnownText.toWKT(this);
    }

}
