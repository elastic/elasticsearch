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

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
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
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;

import static org.elasticsearch.index.mapper.GeoShapeIndexer.toLucenePolygon;

public class VectorGeoPointShapeQueryProcessor implements AbstractGeometryFieldMapper.QueryProcessor {

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        validateIsGeoPointFieldType(fieldName, context);
        // geo points only support intersects
        if (relation != ShapeRelation.INTERSECTS) {
            throw new QueryShardException(context,
                relation+ " query relation not supported for Field [" + fieldName + "].");
        }
        // wrap geoQuery as a ConstantScoreQuery
        return getVectorQueryFromShape(shape, fieldName, relation, context);
    }

    private void validateIsGeoPointFieldType(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType == false) {
            throw new QueryShardException(context, "Expected " + GeoPointFieldMapper.CONTENT_TYPE
                + " field type for Field [" + fieldName + "] but found " + fieldType.typeName());
        }
    }

    protected Query getVectorQueryFromShape(
        Geometry queryShape, String fieldName, ShapeRelation relation, QueryShardContext context) {
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
            return LatLonPoint.newDistanceQuery(fieldName, circle.getLat(), circle.getLon(), circle.getRadiusMeters());
        }

        @Override
        public Query visit(GeometryCollection<?> collection) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            visit(bqb, collection);
            return bqb.build();
        }

        private void visit(BooleanQuery.Builder bqb, GeometryCollection<?> collection) {
            BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
            for (Geometry shape : collection) {
                bqb.add(shape.visit(this), occur);
            }
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Line line) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + GeoShapeType.LINESTRING + " queries");
        }

        @Override
        // don't think this is called directly
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support  "
                + ShapeType.LINEARRING + " queries");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + GeoShapeType.MULTILINESTRING + " queries");
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support "
                + GeoShapeType.MULTIPOINT + " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            Polygon[] polygons = new Polygon[multiPolygon.size()];
            for (int i = 0; i < multiPolygon.size(); i++) {
                polygons[i] = toLucenePolygon(multiPolygon.get(i));
            }
            return LatLonPoint.newPolygonQuery(fieldName, polygons);
        }

        @Override
        public Query visit(Point point) {
            // not currently supported
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + GeoShapeType.POINT +
                " queries");
        }

        @Override
        public Query visit(org.elasticsearch.geometry.Polygon polygon) {
            return LatLonPoint.newPolygonQuery(fieldName, toLucenePolygon(polygon));
        }

        @Override
        public Query visit(Rectangle r) {
            return LatLonPoint.newBoxQuery(fieldName, r.getMinY(), r.getMaxY(), r.getMinX(), r.getMaxX());
        }
    }

}

