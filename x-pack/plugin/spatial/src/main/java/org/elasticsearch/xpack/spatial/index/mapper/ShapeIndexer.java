/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.index.IndexableField;
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
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.xpack.spatial.common.ShapeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShapeIndexer implements AbstractShapeGeometryFieldMapper.Indexer<Geometry, Geometry> {
    private final String name;

    public ShapeIndexer(String name) {
        this.name = name;
    }

    @Override
    public Geometry prepareForIndexing(Geometry geometry) {
        return geometry;
    }

    @Override
    public Class<Geometry> processedClass() {
        return Geometry.class;
    }

    @Override
    public List<IndexableField> indexShape(ParseContext context, Geometry shape) {
        LuceneGeometryVisitor visitor = new LuceneGeometryVisitor(name);
        shape.visit(visitor);
        return visitor.fields;
    }

    private class LuceneGeometryVisitor implements GeometryVisitor<Void, RuntimeException> {
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
            addFields(XYShape.createIndexableFields(name, ShapeUtils.toLuceneXYLine(line)));
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
            for(Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for(Polygon polygon : multiPolygon) {
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
            addFields(XYShape.createIndexableFields(name, ShapeUtils.toLuceneXYPolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            addFields(XYShape.createIndexableFields(name, ShapeUtils.toLuceneXYPolygon(r)));
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }
}
