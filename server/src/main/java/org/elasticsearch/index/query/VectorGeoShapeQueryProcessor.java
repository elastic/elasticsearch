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

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoShapeType;
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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;

import static org.elasticsearch.index.mapper.GeoShapeIndexer.toLucenePolygon;

public class VectorGeoShapeQueryProcessor implements AbstractGeometryFieldMapper.QueryProcessor {

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
        if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
        }
        // wrap geoQuery as a ConstantScoreQuery
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    protected Query getVectorQueryFromShape(Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        GeoShapeIndexer geometryIndexer = new GeoShapeIndexer(true, fieldName);

        Geometry processedShape = geometryIndexer.prepareForIndexing(queryShape);

        if (processedShape == null) {
            return new MatchNoDocsQuery();
        }
        return processedShape.visit(new ShapeVisitor(context, fieldName, relation));
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
            BooleanClause.Occur occur;
            if (relation == ShapeRelation.CONTAINS || relation == ShapeRelation.DISJOINT) {
                // all shapes must be disjoint / must be contained in relation to the indexed shape.
                occur = BooleanClause.Occur.MUST;
            } else {
                // at least one shape must intersect / contain the indexed shape.
                occur = BooleanClause.Occur.SHOULD;
            }
            for (Geometry shape : collection) {
                if (shape instanceof MultiPoint) {
                    // Flatten multi-points
                    // We do not support multi-point queries?
                    visit(bqb, (GeometryCollection<?>) shape);
                } else {
                    bqb.add(shape.visit(this), occur);
                }
            }
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Line line) {
            validateIsGeoShapeFieldType();
            return LatLonShape.newLineQuery(fieldName, relation.getLuceneRelation(), new Line(line.getY(), line.getX()));
        }

        @Override
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            validateIsGeoShapeFieldType();
            Line[] lines = new Line[multiLine.size()];
            for (int i = 0; i < multiLine.size(); i++) {
                lines[i] = new Line(multiLine.get(i).getY(), multiLine.get(i).getX());
            }
            return LatLonShape.newLineQuery(fieldName, relation.getLuceneRelation(), lines);
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + GeoShapeType.MULTIPOINT +
                " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            Polygon[] polygons = new Polygon[multiPolygon.size()];
            for (int i = 0; i < multiPolygon.size(); i++) {
                polygons[i] = toLucenePolygon(multiPolygon.get(i));
            }
            return LatLonShape.newPolygonQuery(fieldName, relation.getLuceneRelation(), polygons);
        }

        @Override
        public Query visit(Point point) {
            validateIsGeoShapeFieldType();
            ShapeField.QueryRelation luceneRelation = relation.getLuceneRelation();
            if (luceneRelation == ShapeField.QueryRelation.CONTAINS) {
                // contains and intersects are equivalent but the implementation of
                // intersects is more efficient.
                luceneRelation = ShapeField.QueryRelation.INTERSECTS;
            }
            return LatLonShape.newBoxQuery(fieldName, luceneRelation,
                point.getY(), point.getY(), point.getX(), point.getX());
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Polygon polygon) {
            return LatLonShape.newPolygonQuery(fieldName, relation.getLuceneRelation(), toLucenePolygon(polygon));
        }

        @Override
        public Query visit(Rectangle r) {
            return LatLonShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                r.getMinY(), r.getMaxY(), r.getMinX(), r.getMaxX());
        }

        private void validateIsGeoShapeFieldType() {
            if (fieldType instanceof GeoShapeFieldMapper.GeoShapeFieldType == false) {
                throw new QueryShardException(context, "Expected " + GeoShapeFieldMapper.CONTENT_TYPE
                    + " field type for Field [" + fieldName + "] but found " + fieldType.typeName());
            }
        }
    }

}

