/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

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

/**
 * Counts the number of Java geometry objects in a geometry tree.
 * Unlike {@link GeometryPointCountVisitor}, this counts the geometry node objects themselves
 * (not the coordinate points they contain), so that even empty sub-geometries (e.g. an empty
 * {@code MultiPoint}) contribute to the total. Useful for estimating per-object heap overhead.
 *
 * <p>Implementation notes:
 * <ul>
 *   <li>{@link MultiPoint} extends {@link GeometryCollection}{@code <Point>} and stores each
 *       point as a distinct Java {@link Point} object, so an N-point {@code MultiPoint} counts
 *       as {@code 1 + N}.</li>
 *   <li>{@link Polygon} owns a separate outer {@link LinearRing} object plus one per hole,
 *       so it counts as {@code 2 + numberOfHoles}.</li>
 *   <li>All other collection types ({@link GeometryCollection}, {@link MultiLine},
 *       {@link MultiPolygon}) count as 1 for the collection plus the recursive count of each
 *       element.</li>
 * </ul>
 */
public class GeometryNodeCountVisitor implements GeometryVisitor<Integer, RuntimeException> {

    @Override
    public Integer visit(Circle circle) throws RuntimeException {
        return 1;
    }

    @Override
    public Integer visit(GeometryCollection<?> collection) throws RuntimeException {
        int count = 1; // the collection object itself
        for (Geometry geometry : collection) {
            count += geometry.visit(this);
        }
        return count;
    }

    @Override
    public Integer visit(Line line) throws RuntimeException {
        return 1;
    }

    @Override
    public Integer visit(LinearRing ring) throws RuntimeException {
        return 1;
    }

    @Override
    public Integer visit(MultiLine multiLine) throws RuntimeException {
        return visit((GeometryCollection<Line>) multiLine);
    }

    @Override
    public Integer visit(MultiPoint multiPoint) throws RuntimeException {
        // MultiPoint extends GeometryCollection<Point> and stores individual Point objects.
        return visit((GeometryCollection<Point>) multiPoint);
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
        // The Polygon object + the outer LinearRing + one LinearRing per hole.
        return 2 + polygon.getNumberOfHoles();
    }

    @Override
    public Integer visit(Rectangle rectangle) throws RuntimeException {
        return 1;
    }
}
