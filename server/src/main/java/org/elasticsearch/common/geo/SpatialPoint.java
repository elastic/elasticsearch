/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

/**
 * To facilitate maximizing the use of common code between GeoPoint and projected CRS
 * we introduced this ElasticPoint as an interface of commonality.
 */
public class SpatialPoint implements Comparable<SpatialPoint> {

    protected double x;
    protected double y;

    public SpatialPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public SpatialPoint(SpatialPoint template) {
        this(template.getX(), template.getY());
    }

    public final double getX() {
        return x;
    }

    public final double getY() {
        return y;
    }

    @Override
    public int hashCode() {
        return 31 * 31 * getClass().getSimpleName().hashCode() + 31 * Double.hashCode(x) + Double.hashCode(y);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SpatialPoint point = (SpatialPoint) obj;
        return (Double.compare(point.x, x) == 0) && Double.compare(point.y, y) == 0;
    }

    @Override
    public String toString() {
        return toWKT();
    }

    public String toWKT() {
        // Code designed to mimic WellKnownText.toWKT, with much less stack depth and object creation
        return "POINT (" + x + " " + y + ")";
    }

    @Override
    public int compareTo(SpatialPoint other) {
        if (this.getClass().equals(other.getClass())) {
            double xd = this.getX() - other.getX();
            double yd = this.getY() - other.getY();
            return (xd == 0) ? comparison(yd) : comparison(xd);
        } else {
            // TODO: Rather separate based on CRS, but since we don't have that yet, we use class name
            // The sort order here is unimportant and does not (yet) introduce BWC issues, so we are free to change it later with CRS
            return this.getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
        }
    }

    private int comparison(double delta) {
        return delta == 0 ? 0 : delta < 0 ? -1 : 1;
    }
}
