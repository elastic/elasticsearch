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
 * Collection of polygons
 */
public class MultiPolygon extends GeometryCollection<Polygon> {
    public static final MultiPolygon EMPTY = new MultiPolygon();

    private MultiPolygon() {}

    public MultiPolygon(List<Polygon> polygons) {
        super(polygons);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTIPOLYGON;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }
}
