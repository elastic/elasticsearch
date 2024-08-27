/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.XYQueriesUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.CartesianPointIndexFieldData;
import org.elasticsearch.xpack.spatial.script.field.CartesianPointDocValuesField;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Field Mapper for point type.
 *
 * Uses lucene 8 XYPoint encoding
 */
public class PointFieldMapper extends AbstractPointGeometryFieldMapper<CartesianPoint> {
    public static final String CONTENT_TYPE = "point";

    private static Builder builder(FieldMapper in) {
        return ((PointFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> builder(m).stored.get(), false);

        final Parameter<Explicit<Boolean>> ignoreMalformed;
        final Parameter<Explicit<Boolean>> ignoreZValue = ignoreZValueParam(m -> builder(m).ignoreZValue.get());
        final Parameter<CartesianPoint> nullValue;
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, boolean ignoreMalformedByDefault) {
            super(name);
            this.ignoreMalformed = ignoreMalformedParam(m -> builder(m).ignoreMalformed.get(), ignoreMalformedByDefault);
            this.nullValue = nullValueParam(
                m -> builder(m).nullValue.get(),
                (n, c, o) -> o == null ? null : parseNullValue(o, ignoreZValue.get().value(), ignoreMalformed.get().value()),
                () -> null,
                XContentBuilder::field
            ).acceptsNull();
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { indexed, hasDocValues, stored, ignoreMalformed, ignoreZValue, nullValue, meta };
        }

        private static CartesianPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            CartesianPoint point = CartesianPoint.parsePoint(nullValue, ignoreZValue);
            if (ignoreMalformed == false) {
                if (Double.isFinite(point.getX()) == false) {
                    throw new IllegalArgumentException("illegal x value [" + point.getX() + "]");
                }
                if (Double.isFinite(point.getY()) == false) {
                    throw new IllegalArgumentException("illegal y value [" + point.getY() + "]");
                }
            }
            return point;
        }

        @Override
        public FieldMapper build(MapperBuilderContext context) {
            if (multiFieldsBuilder.hasMultiFields()) {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.MAPPINGS,
                    "point_multifields",
                    "Adding multifields to [point] mappers has no effect and will be forbidden in future"
                );
            }
            CartesianPointParser parser = new CartesianPointParser(
                leafName(),
                p -> CartesianPoint.parsePoint(p, ignoreZValue.get().value()),
                nullValue.get(),
                ignoreZValue.get().value(),
                ignoreMalformed.get().value()
            );
            PointFieldType ft = new PointFieldType(
                context.buildFullName(leafName()),
                indexed.get(),
                stored.get(),
                hasDocValues.get(),
                parser,
                nullValue.get(),
                meta.get()
            );
            return new PointFieldMapper(leafName(), ft, multiFieldsBuilder.build(this, context), copyTo, parser, this);
        }

    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;

    public PointFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        CartesianPointParser parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            multiFields,
            builder.ignoreMalformed.get(),
            builder.ignoreZValue.get(),
            builder.nullValue.get(),
            copyTo,
            parser
        );
        this.builder = builder;
    }

    @Override
    protected void index(DocumentParserContext context, CartesianPoint point) {
        if (fieldType().isIndexed()) {
            context.doc().add(new XYPointField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new XYDocValuesField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        } else if (fieldType().isStored() || fieldType().isIndexed()) {
            context.addToFieldNames(fieldType().name());
        }
        if (fieldType().isStored()) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public PointFieldType fieldType() {
        return (PointFieldType) mappedFieldType;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), builder.ignoreMalformed.getDefaultValue().value()).init(this);
    }

    public static class PointFieldType extends AbstractPointFieldType<CartesianPoint> implements ShapeQueryable {

        private PointFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            CartesianPointParser parser,
            CartesianPoint nullValue,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, parser, nullValue, meta);
        }

        // only used in test
        public PointFieldType(String name) {
            this(name, true, false, true, null, null, Collections.emptyMap());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new CartesianPointIndexFieldData.Builder(
                name(),
                CartesianPointValuesSourceType.instance(),
                CartesianPointDocValuesField::new
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            return XYQueriesUtils.toXYPointQuery(shape, fieldName, relation, isIndexed(), hasDocValues());
        }

        @Override
        protected Function<List<CartesianPoint>, List<Object>> getFormatter(String format) {
            return GeometryFormatterFactory.getFormatter(format, p -> new Point(p.getX(), p.getY()));
        }
    }

    /** CartesianPoint parser implementation */
    private static class CartesianPointParser extends PointParser<CartesianPoint> {

        CartesianPointParser(
            String field,
            CheckedFunction<XContentParser, CartesianPoint, IOException> objectParser,
            CartesianPoint nullValue,
            boolean ignoreZValue,
            boolean ignoreMalformed
        ) {
            super(field, objectParser, nullValue, ignoreZValue, ignoreMalformed, true);
        }

        @Override
        protected CartesianPoint validate(CartesianPoint in) {
            if (ignoreMalformed == false) {
                if (Double.isFinite(in.getX()) == false) {
                    throw new IllegalArgumentException("illegal x value [" + in.getX() + "] for " + field);
                }
                if (Double.isFinite(in.getY()) == false) {
                    throw new IllegalArgumentException("illegal y value [" + in.getY() + "] for " + field);
                }
            }
            return in;
        }

        @Override
        protected CartesianPoint createPoint(double x, double y) {
            return new CartesianPoint(x, y);
        }

        @Override
        public CartesianPoint normalizeFromSource(CartesianPoint point) {
            return point;
        }
    }
}
