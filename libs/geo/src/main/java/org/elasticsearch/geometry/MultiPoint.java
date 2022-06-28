/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import java.util.List;

/**
 * Represents a MultiPoint object on the earth's surface in decimal degrees and optional altitude in meters.
 */
public class MultiPoint extends GeometryCollection<Point> {
    public static final MultiPoint EMPTY = new MultiPoint();

    private MultiPoint() {}

    public MultiPoint(List<Point> points) {
        super(points);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTIPOINT;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

}
