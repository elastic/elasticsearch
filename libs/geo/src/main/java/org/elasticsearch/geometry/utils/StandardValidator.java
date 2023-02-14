/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
 * Validator that only checks that altitude only shows up if ignoreZValue is set to true.
 */
public class StandardValidator implements GeometryValidator {

    private static final GeometryValidator TRUE = new StandardValidator(true);
    private static final GeometryValidator FALSE = new StandardValidator(false);

    private final boolean ignoreZValue;

    private StandardValidator(boolean ignoreZValue) {
        this.ignoreZValue = ignoreZValue;
    }

    public static GeometryValidator instance(boolean ignoreZValue) {
        return ignoreZValue ? TRUE : FALSE;
    }

    protected void checkZ(double zValue) {
        if (ignoreZValue == false && Double.isNaN(zValue) == false) {
            throw new IllegalArgumentException("found Z value [" + zValue + "] but [ignore_z_value] parameter is [" + ignoreZValue + "]");
        }
    }

    @Override
    public void validate(Geometry geometry) {
        if (ignoreZValue == false) {
            geometry.visit(new GeometryVisitor<Void, RuntimeException>() {

                @Override
                public Void visit(Circle circle) throws RuntimeException {
                    checkZ(circle.getZ());
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
                    for (int i = 0; i < line.length(); i++) {
                        checkZ(line.getZ(i));
                    }
                    return null;
                }

                @Override
                public Void visit(LinearRing ring) throws RuntimeException {
                    for (int i = 0; i < ring.length(); i++) {
                        checkZ(ring.getZ(i));
                    }
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
                    checkZ(point.getZ());
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
                    checkZ(rectangle.getMinZ());
                    checkZ(rectangle.getMaxZ());
                    return null;
                }
            });
        }
    }
}
