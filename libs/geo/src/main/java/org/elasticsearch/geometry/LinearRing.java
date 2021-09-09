/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import java.util.Arrays;

/**
 * Represents a closed line on the earth's surface in lat/lon decimal degrees and optional altitude in meters.
 * <p>
 * Cannot be serialized by WKT directly but used as a part of polygon
 */
public class LinearRing extends Line {
    public static final LinearRing EMPTY = new LinearRing();

    private LinearRing() {
    }

    public LinearRing(double[] x, double[] y) {
        this(x, y, null);
    }

    public LinearRing(double[] x, double[] y, double[] z) {
        super(x, y, z);
        if (x.length < 2) {
            throw new IllegalArgumentException("linear ring cannot contain less than 2 points, found " + x.length);
        }
        int last = x.length - 1;
        if (x[0] != x[last] || y[0] != y[last] || (z != null && z[0] != z[last])) {
            throw new IllegalArgumentException("first and last points of the linear ring must be the same (it must close itself):" +
                " x[0]=" + x[0] + " x[" + last + "]=" + x[last] +
                " y[0]=" + y[0] + " y[" + last + "]=" + y[last] +
                (z == null ? "" : " z[0]=" + z[0] + " z[" + last + "]=" + z[last] ));
        }
    }

    @Override
    public ShapeType type() {
        return ShapeType.LINEARRING;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "linearring(x=" + Arrays.toString(getX()) +
            ", y=" + Arrays.toString(getY()) +
            (hasZ() ? ", z=" + Arrays.toString(getZ()) : "") + ")";
    }
}
