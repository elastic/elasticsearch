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
 * Validator for cartesian (non-geographic) coordinates. Its one job today is
 * {@link #validateBBox(double, double, double, double)}: unlike geographic coordinates, cartesian coordinates
 * have no antimeridian-crossing concept, so a rectangle's x-ordinates must be consistent (maxX cannot be less
 * than minX), in addition to the same y-ordinate check ({@link GeographyValidator} also applies to geographic
 * coordinates. This is deliberately a separate class from {@link StandardValidator}, even though the two
 * currently look similar in scope: {@link StandardValidator} is CRS-agnostic and is relied on by real
 * geographic production code paths (e.g. {@code GeometryParser}) that must tolerate antimeridian-crossing
 * envelopes, so it cannot safely gain this check.
 */
public class CartesianValidator implements GeometryValidator {

    public static final GeometryValidator INSTANCE = new CartesianValidator();

    private CartesianValidator() {}

    @Override
    public void validateBBox(double minX, double maxX, double maxY, double minY) {
        if (maxX < minX) {
            throw new IllegalArgumentException("max x cannot be less than min x");
        }
        GeometryValidator.super.validateBBox(minX, maxX, maxY, minY);
    }

    @Override
    public void validate(Geometry geometry) {
        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {

            @Override
            public Void visit(Circle circle) throws RuntimeException {
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) throws RuntimeException {
                for (Geometry g : collection) {
                    g.visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Line line) throws RuntimeException {
                return null;
            }

            @Override
            public Void visit(LinearRing ring) throws RuntimeException {
                return null;
            }

            @Override
            public Void visit(MultiLine multiLine) throws RuntimeException {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public Void visit(MultiPoint multiPoint) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPoint);
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public Void visit(Point point) throws RuntimeException {
                return null;
            }

            @Override
            public Void visit(Polygon polygon) throws RuntimeException {
                polygon.getPolygon().visit(this);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygon.getHole(i).visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) throws RuntimeException {
                validateBBox(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMaxY(), rectangle.getMinY());
                return null;
            }
        });
    }
}
