/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

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

import java.util.ArrayList;
import java.util.List;

public class PointFactory {

    private PointExtractor pointExtractor = new PointExtractor();

    public PointFactory() {
    }

    public List<Point> getPoints(Geometry geometry) {
        pointExtractor.points.clear();
        geometry.visit(pointExtractor);
        return pointExtractor.points;
    }

    private static class PointExtractor implements GeometryVisitor<Void, IllegalArgumentException> {

        List<Point> points = new ArrayList<>();

        PointExtractor() {
        }

        @Override
        public Void visit(Point point) throws IllegalArgumentException {
            points.add(point);
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) throws IllegalArgumentException {
            for (Point p : multiPoint) {
                visit(p);
            }
            return null;
        }


        @Override
        public Void visit(GeometryCollection<?> collection) throws IllegalArgumentException {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Circle circle)  {
            return null;
        }

        @Override
        public Void visit(Line line) throws IllegalArgumentException {
            return null;
        }

        @Override
        public Void visit(LinearRing ring) throws IllegalArgumentException {
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) throws IllegalArgumentException {
            return null;
        }


        @Override
        public Void visit(MultiPolygon multiPolygon) throws IllegalArgumentException {
            return null;
        }

        @Override
        public Void visit(Polygon polygon) throws IllegalArgumentException {
            return null;
        }

        @Override
        public Void visit(Rectangle rectangle) throws IllegalArgumentException {
            return null;
        }
    }
}
