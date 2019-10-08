/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.ShapeRelation;
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
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import static org.elasticsearch.xpack.spatial.index.mapper.ShapeIndexer.toLucenePolygon;

public class ShapeQueryProcessor implements AbstractGeometryFieldMapper.QueryProcessor {

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        // CONTAINS queries are not yet supported by VECTOR strategy
        if (relation == ShapeRelation.CONTAINS) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]");
        }
        if (shape == null) {
            return new MatchNoDocsQuery();
        }
        // wrap geometry Query as a ConstantScoreQuery
        return new ConstantScoreQuery(shape.visit(new ShapeVisitor(context, fieldName, relation)));
    }

    private class ShapeVisitor implements GeometryVisitor<Query, RuntimeException> {
        QueryShardContext context;
        MappedFieldType fieldType;
        String fieldName;
        ShapeRelation relation;

        ShapeVisitor(QueryShardContext context, String fieldName, ShapeRelation relation) {
            this.context = context;
            this.fieldType = context.fieldMapper(fieldName);
            this.fieldName = fieldName;
            this.relation = relation;
        }

        @Override
        public Query visit(Circle circle) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unknown shape Circle");
        }

        @Override
        public Query visit(GeometryCollection<?> collection) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            visit(bqb, collection);
            return bqb.build();
        }

        private void visit(BooleanQuery.Builder bqb, GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                if (shape instanceof MultiPoint) {
                    // Flatten multipoints
                    visit(bqb, (GeometryCollection<?>) shape);
                } else {
                    bqb.add(shape.visit(this), BooleanClause.Occur.SHOULD);
                }
            }
        }

        @Override
        public Query visit(Line line) {
            return XYShape.newLineQuery(fieldName, relation.getLuceneRelation(),
                new XYLine(doubleArrayToFloatArray(line.getX()), doubleArrayToFloatArray(line.getY())));
        }

        @Override
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            XYLine[] lines = new XYLine[multiLine.size()];
            for (int i=0; i<multiLine.size(); i++) {
                lines[i] = new XYLine(doubleArrayToFloatArray(multiLine.get(i).getX()),
                    doubleArrayToFloatArray(multiLine.get(i).getY()));
            }
            return XYShape.newLineQuery(fieldName, relation.getLuceneRelation(), lines);
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + GeoShapeType.MULTIPOINT +
                " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            XYPolygon[] polygons = new XYPolygon[multiPolygon.size()];
            for (int i=0; i<multiPolygon.size(); i++) {
                polygons[i] = toLucenePolygon(multiPolygon.get(i));
            }
            return visitMultiPolygon(polygons);
        }

        private Query visitMultiPolygon(XYPolygon... polygons) {
            return XYShape.newPolygonQuery(fieldName, relation.getLuceneRelation(), polygons);
        }

        @Override
        public Query visit(Point point) {
            return XYShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                (float)point.getX(), (float)point.getX(), (float)point.getY(), (float)point.getY());
        }

        @Override
        public Query visit(Polygon polygon) {
            return XYShape.newPolygonQuery(fieldName, relation.getLuceneRelation(), toLucenePolygon(polygon));
        }

        @Override
        public Query visit(Rectangle r) {
            return XYShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                (float)r.getMinX(), (float)r.getMaxX(), (float)r.getMinY(), (float)r.getMaxY());
        }
    }

    private static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }
}
