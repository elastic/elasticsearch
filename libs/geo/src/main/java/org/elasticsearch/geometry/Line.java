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

import java.util.Arrays;

/**
 * Represents a Line on the earth's surface in lat/lon decimal degrees and optional altitude in meters.
 */
public class Line implements Geometry {
    public static final Line EMPTY = new Line();
    private final double[] y;
    private final double[] x;
    private final double[] z;

    protected Line() {
        y = new double[0];
        x = new double[0];
        z = null;
    }

    public Line(double[] x, double[] y) {
        this(x, y, null);
    }

    public Line(double[] x, double[] y, double[] z) {
        this.y = y;
        this.x = x;
        this.z = z;
        if (y == null) {
            throw new IllegalArgumentException("y must not be null");
        }
        if (x == null) {
            throw new IllegalArgumentException("x must not be null");
        }
        if (y.length != x.length) {
            throw new IllegalArgumentException("x and y must be equal length");
        }
        if (y.length < 2) {
            throw new IllegalArgumentException("at least two points in the line is required");
        }
        if (z != null && z.length != x.length) {
            throw new IllegalArgumentException("z and x must be equal length");
        }
    }

    public int length() {
        return y.length;
    }

    public double getY(int i) {
        return y[i];
    }

    public double getX(int i) {
        return x[i];
    }

    public double getZ(int i) {
        if (z != null) {
            return z[i];
        } else {
            return Double.NaN;
        }
    }

    public double[] getY() {
        return y.clone();
    }

    public double[] getX() {
        return x.clone();
    }

    public double[] getZ() {
        return z == null ? null : z.clone();
    }

    public double getLat(int i) {
        return y[i];
    }

    public double getLon(int i) {
        return x[i];
    }

    public double getAlt(int i) {
        if (z != null) {
            return z[i];
        } else {
            return Double.NaN;
        }
    }

    public double[] getLats() {
        return y.clone();
    }

    public double[] getLons() {
        return x.clone();
    }

    public double[] getAlts() {
        return z == null ? null : z.clone();
    }

    @Override
    public ShapeType type() {
        return ShapeType.LINESTRING;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return y.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Line line = (Line) o;
        return Arrays.equals(y, line.y) && Arrays.equals(x, line.x) && Arrays.equals(z, line.z);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(y);
        result = 31 * result + Arrays.hashCode(x);
        result = 31 * result + Arrays.hashCode(z);
        return result;
    }

    @Override
    public boolean hasZ() {
        return z != null;
    }

    @Override
    public String toString() {
        return WellKnownText.toWKT(this);
    }
}
