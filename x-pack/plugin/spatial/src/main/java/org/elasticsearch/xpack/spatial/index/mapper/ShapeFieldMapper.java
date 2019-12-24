/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYShape;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;

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
            fieldType().setGeometryQueryBuilder(new ShapeQueryProcessor());
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
