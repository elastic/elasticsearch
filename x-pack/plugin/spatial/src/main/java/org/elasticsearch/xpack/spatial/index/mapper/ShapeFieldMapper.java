/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.util.Map;

import static org.elasticsearch.xpack.spatial.index.mapper.ShapeIndexer.toLucenePolygon;

/**
 * FieldMapper for indexing cartesian {@link XYShape}s.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [1050.0, -1000.0], [1051.0, -1000.0], [1051.0, -1001.0], [1050.0, -1001.0], [1050.0, -1000.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((1050.0 -1000.0, 1051.0 -1000.0, 1051.0 -1001.0, 1050.0 -1001.0, 1050.0 -1000.0))
 */
public class ShapeFieldMapper extends AbstractGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "shape";

    public static class Defaults extends AbstractGeometryFieldMapper.Defaults {
        public static final ShapeFieldType FIELD_TYPE = new ShapeFieldType();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, ShapeFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public ShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new ShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        public ShapeFieldType fieldType() {
            return (ShapeFieldType)fieldType;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeometryParser geometryParser = new GeometryParser(orientation == ShapeBuilder.Orientation.RIGHT,
                coerce(context).value(), ignoreZValue().value());

            fieldType().setGeometryIndexer(new ShapeIndexer(fieldType().name()));
            fieldType().setGeometryParser((parser, mapper) -> geometryParser.parse(parser));
        }
    }

    public static class TypeParser extends AbstractGeometryFieldMapper.TypeParser {
        @Override
        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry,
                                                  Map<String, Object> params) throws MapperParsingException {
            return false;
        }

        @Override
        public Builder newBuilder(String name, Map<String, Object> params) {
            return new Builder(name);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final class ShapeFieldType extends AbstractGeometryFieldType {
        public ShapeFieldType() {
            super();
        }

        public ShapeFieldType(ShapeFieldType ref) {
            super(ref);
        }

        @Override
        public ShapeFieldType clone() {
            return new ShapeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected Indexer<Geometry, Geometry> geometryIndexer() {
            return geometryIndexer;
        }

        @Override
        public Query spatialQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
            }
            if (shape == null) {
                return new MatchNoDocsQuery();
            }
            return shape.visit(new ShapeVisitor(context, fieldName, relation));
        }

    }

    public ShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            Explicit<Boolean> ignoreZValue, Settings indexSettings,
                            MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ShapeFieldType fieldType() {
        return (ShapeFieldType) super.fieldType();
    }

    private static class ShapeVisitor implements GeometryVisitor<Query, RuntimeException> {
        QueryShardContext context;
        String fieldName;
        ShapeRelation relation;

        ShapeVisitor(QueryShardContext context, String fieldName, ShapeRelation relation) {
            this.context = context;
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
            BooleanClause.Occur occur;
            if (relation == ShapeRelation.CONTAINS || relation == ShapeRelation.DISJOINT) {
                // all shapes must be disjoint / must be contained in relation to the indexed shape.
                occur = BooleanClause.Occur.MUST;
            } else {
                // at least one shape must intersect / contain the indexed shape.
                occur = BooleanClause.Occur.SHOULD;
            }
            for (Geometry shape : collection) {
                bqb.add(shape.visit(this), occur);
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
            float[][] points = new float[multiPoint.size()][2];
            for (int i = 0; i < multiPoint.size(); i++) {
                points[i] = new float[] {(float) multiPoint.get(i).getX(), (float) multiPoint.get(i).getY()};
            }
            return XYShape.newPointQuery(fieldName, relation.getLuceneRelation(), points);
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
            ShapeField.QueryRelation luceneRelation = relation.getLuceneRelation();
            if (luceneRelation == ShapeField.QueryRelation.CONTAINS) {
                // contains and intersects are equivalent but the implementation of
                // intersects is more efficient.
                luceneRelation = ShapeField.QueryRelation.INTERSECTS;
            }
            float[][] pointArray  = new float[][] {{(float)point.getX(), (float)point.getY()}};
            return XYShape.newPointQuery(fieldName, luceneRelation, pointArray);
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
