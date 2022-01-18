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
 * Represents a lat/lon rectangle in decimal degrees and optional altitude in meters.
 */
public class Rectangle implements Geometry {
    public static final Rectangle EMPTY = new Rectangle();
    /**
     * minimum latitude value (in degrees)
     */
    private final double minY;
    /**
     * minimum longitude value (in degrees)
     */
    private final double minX;
    /**
     * maximum altitude value (in meters)
     */
    private final double minZ;
    /**
     * maximum latitude value (in degrees)
     */
    private final double maxY;
    /**
     * minimum longitude value (in degrees)
     */
    private final double maxX;
    /**
     * minimum altitude value (in meters)
     */
    private final double maxZ;

    private final boolean empty;

    private Rectangle() {
        minY = 0;
        minX = 0;
        maxY = 0;
        maxX = 0;
        minZ = Double.NaN;
        maxZ = Double.NaN;
        empty = true;
    }

    /**
     * Constructs a bounding box by first validating the provided latitude and longitude coordinates
     */
    public Rectangle(double minX, double maxX, double maxY, double minY) {
        this(minX, maxX, maxY, minY, Double.NaN, Double.NaN);
    }

    /**
     * Constructs a bounding box by first validating the provided latitude and longitude coordinates
     */
    public Rectangle(double minX, double maxX, double maxY, double minY, double minZ, double maxZ) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minZ = minZ;
        this.maxZ = maxZ;
        empty = false;
        if (maxY < minY) {
            throw new IllegalArgumentException("max y cannot be less than min y");
        }
        if (Double.isNaN(minZ) != Double.isNaN(maxZ)) {
            throw new IllegalArgumentException("only one z value is specified");
        }
    }

    public double getMinY() {
        return minY;
    }

    public double getMinX() {
        return minX;
    }

    public double getMinZ() {
        return minZ;
    }

    public double getMaxY() {
        return maxY;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMaxZ() {
        return maxZ;
    }

    public double getMinLat() {
        return minY;
    }

    public double getMinLon() {
        return minX;
    }

    public double getMinAlt() {
        return minZ;
    }

    public double getMaxLat() {
        return maxY;
    }

    public double getMaxLon() {
        return maxX;
    }

    public double getMaxAlt() {
        return maxZ;
    }

    @Override
    public ShapeType type() {
        return ShapeType.ENVELOPE;
    }

    @Override
    public String toString() {
        return WellKnownText.toWKT(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rectangle rectangle = (Rectangle) o;

        if (Double.compare(rectangle.minY, minY) != 0) return false;
        if (Double.compare(rectangle.minX, minX) != 0) return false;
        if (Double.compare(rectangle.maxY, maxY) != 0) return false;
        if (Double.compare(rectangle.maxX, maxX) != 0) return false;
        if (Double.compare(rectangle.minZ, minZ) != 0) return false;
        return Double.compare(rectangle.maxZ, maxZ) == 0;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(minY);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minX);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxY);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxX);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minZ);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxZ);
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
        return Double.isNaN(maxZ) == false;
    }
}
