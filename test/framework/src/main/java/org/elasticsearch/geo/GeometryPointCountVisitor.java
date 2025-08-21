/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geo;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

public class GeometryPointCountVisitor implements GeometryVisitor<Integer, RuntimeException> {

    @Override
    public Integer visit(Circle circle) throws RuntimeException {
        return 2;
    }

    @Override
    public Integer visit(GeometryCollection<?> collection) throws RuntimeException {
        int size = 0;
        for (Geometry geometry : collection) {
            size += geometry.visit(this);
        }
        return size;
    }

    @Override
    public Integer visit(Line line) throws RuntimeException {
        return line.length();
    }

    @Override
    public Integer visit(LinearRing ring) throws RuntimeException {
        return ring.length();
    }

    @Override
    public Integer visit(MultiLine multiLine) throws RuntimeException {
        return visit((GeometryCollection<Line>) multiLine);
    }

    @Override
    public Integer visit(MultiPoint multiPoint) throws RuntimeException {
        return multiPoint.size();
    }

    @Override
    public Integer visit(MultiPolygon multiPolygon) throws RuntimeException {
        return visit((GeometryCollection<Polygon>) multiPolygon);
    }

    @Override
    public Integer visit(Point point) throws RuntimeException {
        return 1;
    }

    @Override
    public Integer visit(Polygon polygon) throws RuntimeException {
        int size = polygon.getPolygon().length();
        for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
            size += polygon.getHole(i).length();
        }
        return size;
    }

    @Override
    public Integer visit(Rectangle rectangle) throws RuntimeException {
        return 4;
    }
}
