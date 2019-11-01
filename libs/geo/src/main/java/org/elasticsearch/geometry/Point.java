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

package org.elasticsearch.geometry;

import org.elasticsearch.geometry.utils.WellKnownText;

/**
 * Represents a Point on the earth's surface in decimal degrees and optional altitude in meters.
 */
public class Point implements Geometry {
    public static final Point EMPTY = new Point();

    private final double y;
    private final double x;
    private final double z;
    private final boolean empty;

    private Point() {
        y = 0;
        x = 0;
        z = Double.NaN;
        empty = true;
    }

    public Point(double x, double y) {
        this(x, y, Double.NaN);
    }

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

    public double getY() {
        return y;
    }

    public double getX() {
        return x;
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
        return WellKnownText.INSTANCE.toWKT(this);
    }

}
