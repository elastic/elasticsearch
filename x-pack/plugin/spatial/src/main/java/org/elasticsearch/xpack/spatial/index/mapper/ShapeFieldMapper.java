/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
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

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder {

        public Builder(String name) {
            super(name, new FieldType());
            fieldType.setDimensions(7, 4, Integer.BYTES);
        }

        @Override
        public ShapeFieldMapper build(BuilderContext context) {
            GeometryParser geometryParser
                = new GeometryParser(orientation().value().getAsBoolean(), coerce().value(), ignoreZValue().value());
            Parser<Geometry> parser = new GeoShapeParser(geometryParser);
            ShapeFieldType ft = new ShapeFieldType(buildFullName(context), indexed, fieldType.stored(), hasDocValues, parser, meta);
            ft.setOrientation(orientation().value());
            return new ShapeFieldMapper(name, fieldType, ft, ignoreMalformed(context), coerce(context),
                ignoreZValue(), orientation(), multiFieldsBuilder.build(this, context), copyTo,
                new ShapeIndexer(ft.name()), parser);
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

    public static final class ShapeFieldType extends AbstractShapeGeometryFieldType
        implements ShapeQueryable {

        private final ShapeQueryProcessor queryProcessor;

        public ShapeFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                              Parser<Geometry> parser, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, false, parser, meta);
            this.queryProcessor = new ShapeQueryProcessor();
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            return queryProcessor.shapeQuery(shape, fieldName, relation, context);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    public ShapeFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                            Explicit<Boolean> ignoreZValue, Explicit<Orientation> orientation,
                            MultiFields multiFields, CopyTo copyTo,
                            Indexer<Geometry, Geometry> indexer, Parser<Geometry> parser) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation,
            multiFields, copyTo, indexer, parser);
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
