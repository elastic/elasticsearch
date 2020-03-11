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
import org.apache.lucene.geo.LatLonGeometry;
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
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.util.ArrayList;
import java.util.List;

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

        @Override
        public Query spatialQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0)
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(Version.V_7_5_0)) {
                throw new QueryShardException(context,
                    ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "].");
            }
            if (shape == null) {
                return new MatchNoDocsQuery();
            }
            return makeQueryFromGeometry(shape, fieldName, relation, context);
        }

        private static Query makeQueryFromGeometry(Geometry shape, String fieldName,
                                                   ShapeRelation relation, QueryShardContext context) {
            List<LatLonGeometry> geometries = new ArrayList<>();
            shape.visit(new GeometryVisitor<>() {

                @Override
                public Void visit(Circle circle) {
                    throw new QueryShardException(context, "Field [" + fieldName + "] found and unknown shape Circle");
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
                        List<Line> collector = new ArrayList<>();
                        GeoLineDecomposer.decomposeLine(line, collector);
                        collectLines(collector);
                    }
                    return null;
                }

                @Override
                public Void visit(LinearRing ring) {
                    throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
                }

                @Override
                public Void visit(MultiLine multiLine) {
                    List<Line> collector = new ArrayList<>();
                    GeoLineDecomposer.decomposeMultiLine(multiLine, collector);
                    collectLines(collector);
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
                    if (multiPolygon.isEmpty() == false) {
                        List<Polygon> collector = new ArrayList<>();
                        GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, true, collector);
                        collectPolygons(collector);
                    }
                    return null;
                }

                @Override
                public Void visit(Point point) {
                    if (point.isEmpty() == false) {
                        geometries.add(GeoShapeUtils.toLucenePoint(point));
                    }
                    return null;

                }

                @Override
                public Void visit(Polygon polygon) {
                    if (polygon.isEmpty() == false) {
                        List<Polygon> collector = new ArrayList<>();
                        GeoPolygonDecomposer.decomposePolygon(polygon, true, collector);
                        collectPolygons(collector);
                    }
                    return null;
                }

                @Override
                public Void visit(Rectangle r) {
                    if (r.isEmpty() == false) {
                        geometries.add(GeoShapeUtils.toLuceneRectangle(r));
                    }
                    return null;
                }

                private void collectLines(List<Line> geometryLines) {
                    for (Line line: geometryLines) {
                        geometries.add(GeoShapeUtils.toLuceneLine(line));
                    }
                }

                private void collectPolygons(List<Polygon> geometryPolygons) {
                    for (Polygon polygon : geometryPolygons) {
                        geometries.add(GeoShapeUtils.toLucenePolygon(polygon));
                    }
                }
            });
            if (geometries.size() == 0) {
                return new MatchNoDocsQuery();
            }
            return LatLonShape.newGeometryQuery(fieldName, relation.getLuceneRelation(),
                   geometries.toArray(new LatLonGeometry[geometries.size()]));
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
}
