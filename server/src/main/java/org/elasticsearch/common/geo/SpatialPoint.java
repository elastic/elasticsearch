/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import java.util.Locale;

/**
 * To facilitate maximizing the use of common code between GeoPoint and projected CRS
 * we introduced this ElasticPoint as an interface of commonality.
 */
public class SpatialPoint {

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
        return 31 * Double.hashCode(x) + Double.hashCode(y);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SpatialPoint point = (SpatialPoint) obj;
        return (Double.compare(point.x, x) != 0) && Double.compare(point.y, y) == 0;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "POINT (%f %f)", x, y);
    }
}
