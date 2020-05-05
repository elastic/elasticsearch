/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper.AbstractGeometryFieldType.QueryProcessor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.common.ShapeUtils;


public class ShapeQueryPointProcessor implements QueryProcessor {

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        validateIsPointFieldType(fieldName, context);
        // only the intersects relation is supported for indexed cartesian point types
        if (relation != ShapeRelation.INTERSECTS) {
            throw new QueryShardException(context,
                relation+ " query relation not supported for Field [" + fieldName + "].");
        }
        // wrap XYPoint query as a ConstantScoreQuery
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    private void validateIsPointFieldType(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof PointFieldMapper.PointFieldType == false) {
            throw new QueryShardException(context, "Expected " + PointFieldMapper.CONTENT_TYPE
                + " field type for Field [" + fieldName + "] but found " + fieldType.typeName());
        }
    }

    protected Query getVectorQueryFromShape(
        Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        ShapeVisitor shapeVisitor = new ShapeVisitor(context, fieldName, relation);
        return queryShape.visit(shapeVisitor);
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
            XYCircle xyCircle = ShapeUtils.toLuceneXYCircle(circle);
            Query query = XYPointField.newDistanceQuery(fieldName, xyCircle.getX(), xyCircle.getY(), xyCircle.getRadius());
            if (fieldType.hasDocValues()) {
                Query dvQuery = XYDocValuesField.newSlowDistanceQuery(fieldName,
                    xyCircle.getX(), xyCircle.getY(), xyCircle.getRadius());
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }

        @Override
        public Query visit(GeometryCollection<?> collection) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            visit(bqb, collection);
            return bqb.build();
        }

        private void visit(BooleanQuery.Builder bqb, GeometryCollection<?> collection) {
            BooleanClause.Occur occur = BooleanClause.Occur.FILTER;
            for (Geometry shape : collection) {
                bqb.add(shape.visit(this), occur);
            }
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Line line) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + ShapeType.LINESTRING + " queries");
        }

        @Override
        // don't think this is called directly
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + ShapeType.LINEARRING + " queries");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + ShapeType.MULTILINESTRING + " queries");
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + ShapeType.MULTIPOINT + " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            org.apache.lucene.geo.XYPolygon[] lucenePolygons =
                new org.apache.lucene.geo.XYPolygon[multiPolygon.size()];
            for (int i = 0; i < multiPolygon.size(); i++) {
                lucenePolygons[i] = ShapeUtils.toLuceneXYPolygon(multiPolygon.get(i));
            }
            Query query = XYPointField.newPolygonQuery(fieldName, lucenePolygons);
            if (fieldType.hasDocValues()) {
                Query dvQuery = XYDocValuesField.newSlowPolygonQuery(fieldName, lucenePolygons);
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }

        @Override
        public Query visit(Point point) {
            // not currently supported
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + ShapeType.POINT +
                " queries");
        }

        @Override
        public Query visit(Polygon polygon) {
            org.apache.lucene.geo.XYPolygon lucenePolygon = ShapeUtils.toLuceneXYPolygon(polygon);
            Query query = XYPointField.newPolygonQuery(fieldName, lucenePolygon);
            if (fieldType.hasDocValues()) {
                Query dvQuery = XYDocValuesField.newSlowPolygonQuery(fieldName, lucenePolygon);
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }

        @Override
        public Query visit(Rectangle r) {
            XYRectangle xyRectangle = ShapeUtils.toLuceneXYRectangle(r);
            Query query = XYPointField.newBoxQuery(fieldName, xyRectangle.minX, xyRectangle.maxX, xyRectangle.minY, xyRectangle.maxY);
            if (fieldType.hasDocValues()) {
                Query dvQuery = XYDocValuesField.newSlowBoxQuery(
                    fieldName, xyRectangle.minX, xyRectangle.maxX, xyRectangle.minY, xyRectangle.maxY);
                query = new IndexOrDocValuesQuery(query, dvQuery);
            }
            return query;
        }
    }
}
