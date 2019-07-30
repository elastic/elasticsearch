/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShapeIndexer implements AbstractGeometryFieldMapper.Indexer<Geometry, Geometry> {
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
        public Void visit(org.elasticsearch.geo.geometry.Line line) {
            float[][] vertices = lineToFloatArray(line.getLons(), line.getLats());
            addFields(XYShape.createIndexableFields(name, new XYLine(vertices[0], vertices[1])));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (org.elasticsearch.geo.geometry.Line line : multiLine) {
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
            for(org.elasticsearch.geo.geometry.Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            addFields(XYShape.createIndexableFields(name, (float)point.getLon(), (float)point.getLat()));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            addFields(XYShape.createIndexableFields(name, toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Rectangle r) {
            XYPolygon p = new XYPolygon(
                new float[]{(float)r.getMinLon(), (float)r.getMaxLon(), (float)r.getMaxLon(), (float)r.getMinLon(), (float)r.getMinLon()},
                new float[]{(float)r.getMinLat(), (float)r.getMinLat(), (float)r.getMaxLat(), (float)r.getMaxLat(), (float)r.getMinLat()});
            addFields(XYShape.createIndexableFields(name, p));
            return null;
        }

        private void addFields(IndexableField[] fields) {
            this.fields.addAll(Arrays.asList(fields));
        }
    }

    public static XYPolygon toLucenePolygon(org.elasticsearch.geo.geometry.Polygon polygon) {
        XYPolygon[] holes = new XYPolygon[polygon.getNumberOfHoles()];
        LinearRing ring;
        float[][] vertices;
        for(int i = 0; i<holes.length; i++) {
            ring = polygon.getHole(i);
            vertices = lineToFloatArray(ring.getLons(), ring.getLats());
            holes[i] = new XYPolygon(vertices[0], vertices[1]);
        }
        ring = polygon.getPolygon();
        vertices = lineToFloatArray(ring.getLons(), ring.getLats());
        return new XYPolygon(vertices[0], vertices[1], holes);
    }

    private static float[][] lineToFloatArray(double[] x, double[] y) {
        float[][] result = new float[2][x.length];
        for (int i = 0; i < x.length; ++i) {
            result[0][i] = (float)x[i];
            result[1][i] = (float)y[i];
        }
        return result;
    }
}
