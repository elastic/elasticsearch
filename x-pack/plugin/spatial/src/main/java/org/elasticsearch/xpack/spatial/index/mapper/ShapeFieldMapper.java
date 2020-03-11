/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYGeometry;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            return makeQueryFromGeometry(shape, fieldName, relation, context);
        }

        private static Query makeQueryFromGeometry(Geometry shape, String fieldName,
                                                   ShapeRelation relation, QueryShardContext context) {
            List<XYGeometry> geometries = new ArrayList<>();
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
                        geometries.add(ShapeUtils.toLuceneLine(line));
                    }
                    return null;
                }

                @Override
                public Void visit(LinearRing ring) {
                    throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
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
                        geometries.add(ShapeUtils.toLucenePoint(point));
                    }
                    return null;

                }

                @Override
                public Void visit(Polygon polygon) {
                    if (polygon.isEmpty() == false) {
                        geometries.add(ShapeUtils.toLucenePolygon(polygon));
                    }
                    return null;
                }

                @Override
                public Void visit(Rectangle r) {
                    if (r.isEmpty() == false) {
                        geometries.add(ShapeUtils.toLuceneRectangle(r));
                    }
                    return null;
                }
            });
            if (geometries.size() == 0) {
                return new MatchNoDocsQuery();
            }
            return XYShape.newGeometryQuery(fieldName,
                relation.getLuceneRelation(), geometries.toArray(new XYGeometry[geometries.size()]));
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
}
