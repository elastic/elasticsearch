/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.XYShape;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryProcessor;

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
public class ShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "shape";

    @SuppressWarnings({"unchecked"})
    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder, ShapeFieldType> {

        public Builder(String name) {
            super(name, new FieldType());
            fieldType.setDimensions(7, 4, Integer.BYTES);
            builder = this;
        }

        @Override
        public ShapeFieldMapper build(BuilderContext context) {
            ShapeFieldType ft = new ShapeFieldType(buildFullName(context), indexed, hasDocValues, meta);
            GeometryParser geometryParser
                = new GeometryParser(orientation().value().getAsBoolean(), coerce().value(), ignoreZValue().value());
            ft.setGeometryParser(new GeoShapeParser(geometryParser));
            ft.setGeometryIndexer(new ShapeIndexer(ft.name()));
            ft.setGeometryQueryBuilder(new ShapeQueryProcessor());
            ft.setOrientation(orientation().value());
            return new ShapeFieldMapper(name, fieldType, ft, ignoreMalformed(context), coerce(context),
                ignoreZValue(), orientation(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {
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

    public static final class ShapeFieldType extends AbstractShapeGeometryFieldType<Geometry, Geometry> {
        public ShapeFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, hasDocValues, meta);
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

    public ShapeFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            Explicit<Boolean> ignoreZValue, Explicit<Orientation> orientation,
                            MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation,
            multiFields, copyTo);
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?,?> mergeWith, List<String> conflicts) {

    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {
        // noop: we currently do not store geo_shapes
        // @todo store as geojson string?
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void addDocValuesFields(String name, Geometry geometry, List fields, ParseContext context) {
        // we should throw a mapping exception before we get here
    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
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
