/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
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
import org.elasticsearch.index.mapper.ShapeIndexer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility class that converts geometries into Lucene-compatible form for indexing in a shape field.
 */
public class CartesianShapeIndexer implements ShapeIndexer {
    private final String name;

    public CartesianShapeIndexer(String name) {
        this.name = name;
    }

    public List<IndexableField> indexShape(Geometry shape) {
        if (shape == null) {
            return Collections.emptyList();
        }
        LuceneGeometryVisitor visitor = new LuceneGeometryVisitor(name);
        shape.visit(visitor);
        return visitor.fields;
    }

    private static class LuceneGeometryVisitor implements GeometryVisitor<Void, RuntimeException> {
        private List<IndexableField> fields = new ArrayList<>();
        private String name;

        private LuceneGeometryVisitor(String name) {
            this.name = name;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            addFields(XYShape.createIndexableFields(name, LuceneGeometriesUtils.toXYLine(line)));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            addFields(XYShape.createIndexableFields(name, (float) point.getX(), (float) point.getY()));
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            addFields(XYShape.createIndexableFields(name, LuceneGeometriesUtils.toXYPolygon(polygon), true));
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            addFields(XYShape.createIndexableFields(name, toLuceneXYPolygon(r)));
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }

    private static org.apache.lucene.geo.XYPolygon toLuceneXYPolygon(Rectangle r) {
        return new org.apache.lucene.geo.XYPolygon(
            new float[] { (float) r.getMinX(), (float) r.getMaxX(), (float) r.getMaxX(), (float) r.getMinX(), (float) r.getMinX() },
            new float[] { (float) r.getMinY(), (float) r.getMinY(), (float) r.getMaxY(), (float) r.getMaxY(), (float) r.getMinY() }
        );
    }
}
