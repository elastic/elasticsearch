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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
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
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.List;

public class LegacyGeoShapeQueryProcessor implements AbstractGeometryFieldMapper.QueryProcessor {

    private AbstractGeometryFieldMapper.AbstractGeometryFieldType ft;

    public LegacyGeoShapeQueryProcessor(AbstractGeometryFieldMapper.AbstractGeometryFieldType ft) {
        this.ft = ft;
    }

    @Override
    public Query process(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        throw new UnsupportedOperationException("process method should not be called for PrefixTree based geo_shapes");
    }

    @Override
    public Query process(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation, QueryShardContext context) {
        LegacyGeoShapeFieldMapper.GeoShapeFieldType shapeFieldType = (LegacyGeoShapeFieldMapper.GeoShapeFieldType) ft;
        SpatialStrategy spatialStrategy = shapeFieldType.strategy();
        if (strategy != null) {
            spatialStrategy = strategy;
        }
        PrefixTreeStrategy prefixTreeStrategy = shapeFieldType.resolvePrefixTreeStrategy(spatialStrategy);
        if (prefixTreeStrategy instanceof RecursivePrefixTreeStrategy && relation == ShapeRelation.DISJOINT) {
            // this strategy doesn't support disjoint anymore: but it did
            // before, including creating lucene fieldcache (!)
            // in this case, execute disjoint as exists && !intersects
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            Query exists = ExistsQueryBuilder.newFilter(context, fieldName);
            Query intersects = prefixTreeStrategy.makeQuery(getArgs(shape, ShapeRelation.INTERSECTS));
            bool.add(exists, BooleanClause.Occur.MUST);
            bool.add(intersects, BooleanClause.Occur.MUST_NOT);
            return bool.build();
        } else {
            return prefixTreeStrategy.makeQuery(getArgs(shape, relation));
        }
    }

    public static SpatialArgs getArgs(Geometry shape, ShapeRelation relation) {
        switch (relation) {
            case DISJOINT:
                return new SpatialArgs(SpatialOperation.IsDisjointTo, buildS4J(shape));
            case INTERSECTS:
                return new SpatialArgs(SpatialOperation.Intersects, buildS4J(shape));
            case WITHIN:
                return new SpatialArgs(SpatialOperation.IsWithin, buildS4J(shape));
            case CONTAINS:
                return new SpatialArgs(SpatialOperation.Contains, buildS4J(shape));
            default:
                throw new IllegalArgumentException("invalid relation [" + relation + "]");
        }
    }


    /**
     * Builds JTS shape from a geometry
     * <p>
     * This method is needed to handle legacy indices and will be removed when we no longer need to build JTS shapes
     */
    private static Shape buildS4J(Geometry geometry) {
        return geometryToShapeBuilder(geometry).buildS4J();
    }


    public static ShapeBuilder<?, ?, ?> geometryToShapeBuilder(Geometry geometry) {
        ShapeBuilder<?, ?, ?> shapeBuilder = geometry.visit(new GeometryVisitor<>() {
            @Override
            public ShapeBuilder<?, ?, ?> visit(Circle circle) {
                return new CircleBuilder().center(circle.getLon(), circle.getLat()).radius(circle.getRadiusMeters(), DistanceUnit.METERS);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(GeometryCollection<?> collection) {
                GeometryCollectionBuilder shapes = new GeometryCollectionBuilder();
                for (Geometry geometry : collection) {
                    shapes.shape(geometry.visit(this));
                }
                return shapes;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Line line) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    coordinates.add(new Coordinate(line.getX(i), line.getY(i), line.getZ(i)));
                }
                return new LineStringBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(LinearRing ring) {
                throw new UnsupportedOperationException("circle is not supported");
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiLine multiLine) {
                MultiLineStringBuilder lines = new MultiLineStringBuilder();
                for (int i = 0; i < multiLine.size(); i++) {
                    lines.linestring((LineStringBuilder) visit(multiLine.get(i)));
                }
                return lines;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPoint multiPoint) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    coordinates.add(new Coordinate(p.getX(), p.getY(), p.getZ()));
                }
                return new MultiPointBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPolygon multiPolygon) {
                MultiPolygonBuilder polygons = new MultiPolygonBuilder();
                for (int i = 0; i < multiPolygon.size(); i++) {
                    polygons.polygon((PolygonBuilder) visit(multiPolygon.get(i)));
                }
                return polygons;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Point point) {
                return new PointBuilder(point.getX(), point.getY());
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Polygon polygon) {
                PolygonBuilder polygonBuilder =
                    new PolygonBuilder((LineStringBuilder) visit((Line) polygon.getPolygon()),
                        ShapeBuilder.Orientation.RIGHT, false);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygonBuilder.hole((LineStringBuilder) visit((Line) polygon.getHole(i)));
                }
                return polygonBuilder;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Rectangle rectangle) {
                return new EnvelopeBuilder(new Coordinate(rectangle.getMinX(), rectangle.getMaxY()),
                    new Coordinate(rectangle.getMaxX(), rectangle.getMinY()));
            }
        });
        return shapeBuilder;
    }
}
