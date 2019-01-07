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

import java.util.Arrays;

/**
 * Represents a Line on the earth's surface in lat/lon decimal degrees.
 */
public class Line implements Geometry {
    public static final Line EMPTY = new Line();
    private final double[] lats;
    private final double[] lons;

    protected Line() {
        lats = new double[0];
        lons = new double[0];
    }

    public Line(double[] lats, double[] lons) {
        this.lats = lats;
        this.lons = lons;
        if (lats == null) {
            throw new IllegalArgumentException("lats must not be null");
        }
        if (lons == null) {
            throw new IllegalArgumentException("lons must not be null");
        }
        if (lats.length != lons.length) {
            throw new IllegalArgumentException("lats and lons must be equal length");
        }
        if (lats.length < 2) {
            throw new IllegalArgumentException("at least two points in the line is required");
        }
        for (int i = 0; i < lats.length; i++) {
            GeometryUtils.checkLatitude(lats[i]);
            GeometryUtils.checkLongitude(lons[i]);
        }
    }

    public int length() {
        return lats.length;
    }

    public double getLat(int i) {
        return lats[i];
    }

    public double getLon(int i) {
        return lons[i];
    }

    @Override
    public ShapeType type() {
        return ShapeType.LINESTRING;
    }

    @Override
    public <T> T visit(GeometryVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return lats.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Line line = (Line) o;
        return Arrays.equals(lats, line.lats) &&
            Arrays.equals(lons, line.lons);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(lats);
        result = 31 * result + Arrays.hashCode(lons);
        return result;
    }

    @Override
    public String toString() {
        return "lats=" + Arrays.toString(lats) +
            ", lons=" + Arrays.toString(lons);
    }
}
