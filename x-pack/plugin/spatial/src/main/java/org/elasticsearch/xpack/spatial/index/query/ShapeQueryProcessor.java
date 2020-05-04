/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
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
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper.AbstractGeometryFieldType.QueryProcessor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.common.ShapeUtils;

import java.util.ArrayList;
import java.util.List;


public class ShapeQueryProcessor implements QueryProcessor {

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        validateIsShapeFieldType(fieldName, context);
        // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
        if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
        }
        if (shape == null) {
            return new MatchNoDocsQuery();
        }
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    private void validateIsShapeFieldType(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof ShapeFieldMapper.ShapeFieldType == false) {
            throw new QueryShardException(context, "Expected " + ShapeFieldMapper.CONTENT_TYPE
                + " field type for Field [" + fieldName + "] but found " + fieldType.typeName());
        }
    }

    private Query getVectorQueryFromShape(Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        final LuceneGeometryCollector visitor = new LuceneGeometryCollector(fieldName, context);
        queryShape.visit(visitor);
        final List<XYGeometry> geometries = visitor.geometries();
        if (geometries.size() == 0) {
            return new MatchNoDocsQuery();
        }
        return XYShape.newGeometryQuery(fieldName, relation.getLuceneRelation(),
            geometries.toArray(new XYGeometry[geometries.size()]));
    }

    private static class LuceneGeometryCollector implements GeometryVisitor<Void, RuntimeException> {
        private final List<XYGeometry> geometries = new ArrayList<>();
        private final String name;
        private final QueryShardContext context;

        private LuceneGeometryCollector(String name, QueryShardContext context) {
            this.name = name;
            this.context = context;
        }

        List<XYGeometry> geometries() {
            return geometries;
        }

        @Override
        public Void visit(Circle circle) {
            if (circle.isEmpty() == false) {
                geometries.add(ShapeUtils.toLuceneXYCircle(circle));
            }
            return null;
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                shape.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            if (line.isEmpty() == false) {
                geometries.add(ShapeUtils.toLuceneXYLine(line));
            }
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape LinearRing");
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
            if (point.isEmpty() == false) {
                geometries.add(ShapeUtils.toLuceneXYPoint(point));
            }
            return null;

        }

        @Override
        public Void visit(Polygon polygon) {
            if (polygon.isEmpty() == false) {
                geometries.add(ShapeUtils.toLuceneXYPolygon(polygon));
            }
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            if (r.isEmpty() == false) {
                geometries.add(ShapeUtils.toLuceneXYRectangle(r));
            }
            return null;
        }
    }
}
