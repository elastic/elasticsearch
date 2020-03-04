/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeoLineDecomposer;
import org.elasticsearch.common.geo.GeoPolygonDecomposer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.mapper.GeoShapeIndexer.toLucenePolygon;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeFieldMapper extends AbstractGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "geo_shape";

    public static class Builder extends AbstractGeometryFieldMapper.Builder<AbstractGeometryFieldMapper.Builder, GeoShapeFieldMapper> {
        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            GeoShapeFieldType fieldType = (GeoShapeFieldType)fieldType();
            boolean orientation = fieldType.orientation == ShapeBuilder.Orientation.RIGHT;

            GeometryParser geometryParser = new GeometryParser(orientation, coerce(context).value(), ignoreZValue().value());

            fieldType.setGeometryIndexer(new GeoShapeIndexer(orientation, fieldType.name()));
            fieldType.setGeometryParser( (parser, mapper) -> geometryParser.parse(parser));
        }
    }

    public static final class GeoShapeFieldType extends AbstractGeometryFieldType<Geometry, Geometry> {
        public GeoShapeFieldType() {
            super();
        }

        @Override
        public Query geoQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
            }
            if (shape == null) {
                return new MatchNoDocsQuery();
            }
            return shape.visit(new ShapeVisitor(context, fieldName, relation));
        }

        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
        }

        @Override
        public GeoShapeFieldType clone() {
            return new GeoShapeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper) {
            LegacyGeoShapeFieldMapper legacy = (LegacyGeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException("[" + fieldType().name() + "] with field mapper [" + fieldType().typeName() + "] " +
                "using [BKD] strategy cannot be merged with " + "[" + legacy.fieldType().typeName() + "] with [" +
                legacy.fieldType().strategy() + "] strategy");
        }
        super.doMerge(mergeWith);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private static class ShapeVisitor implements GeometryVisitor<Query, RuntimeException> {
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
        public Query visit(org.elasticsearch.geometry.Line line) {
            List<org.elasticsearch.geometry.Line> collector = new ArrayList<>();
            GeoLineDecomposer.decomposeLine(line, collector);
            return makeLineQuery(collector);
        }

        private Query makeLineQuery(List<org.elasticsearch.geometry.Line> geometryLines) {
            Line[] lines  = new Line[geometryLines.size()];
            for (int i = 0; i < geometryLines.size(); i++) {
                lines[i] =  new Line(geometryLines.get(i).getY(), geometryLines.get(i).getX());
            }
            return LatLonShape.newLineQuery(fieldName, relation.getLuceneRelation(), lines);
        }

        private Query makeQuery(List<org.elasticsearch.geometry.Polygon> geometryPolygons) {
            Polygon[] polygons  = new Polygon[geometryPolygons.size()];
            for (int i = 0; i < geometryPolygons.size(); i++) {
                polygons[i] = toLucenePolygon(geometryPolygons.get(i));
            }
            return LatLonShape.newPolygonQuery(fieldName, relation.getLuceneRelation(), polygons);
        }

        @Override
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            List<org.elasticsearch.geometry.Line> collector = new ArrayList<>();
            GeoLineDecomposer.decomposeMultiLine(multiLine, collector);
            return makeLineQuery(collector);
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            double[][] points = new double[multiPoint.size()][2];
            for (int i = 0; i < multiPoint.size(); i++) {
                points[i] = new double[] {multiPoint.get(i).getLat(), multiPoint.get(i).getLon()};
            }
            return LatLonShape.newPointQuery(fieldName, relation.getLuceneRelation(), points);
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            List<org.elasticsearch.geometry.Polygon> collector = new ArrayList<>();
            GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, true, collector);
            return makeQuery(collector);
        }

        @Override
        public Query visit(Point point) {
            ShapeField.QueryRelation luceneRelation = relation.getLuceneRelation();
            if (luceneRelation == ShapeField.QueryRelation.CONTAINS) {
                // contains and intersects are equivalent but the implementation of
                // intersects is more efficient.
                luceneRelation = ShapeField.QueryRelation.INTERSECTS;
            }
            return LatLonShape.newPointQuery(fieldName, luceneRelation,
                new double[] {point.getY(), point.getX()});
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Polygon polygon) {
            List<org.elasticsearch.geometry.Polygon> collector = new ArrayList<>();
            GeoPolygonDecomposer.decomposePolygon(polygon, true, collector);
            return makeQuery(collector);
        }

        @Override
        public Query visit(Rectangle r) {
            return LatLonShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                r.getMinY(), r.getMaxY(), r.getMinX(), r.getMaxX());
        }
    }
}
