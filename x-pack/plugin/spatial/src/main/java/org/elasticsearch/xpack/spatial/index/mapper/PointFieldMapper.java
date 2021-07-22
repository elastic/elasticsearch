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
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.AbstractPointGeometryFieldMapper;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryPointProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Field Mapper for point type.
 *
 * Uses lucene 8 XYPoint encoding
 */
public class PointFieldMapper extends AbstractPointGeometryFieldMapper<CartesianPoint> {
    public static final String CONTENT_TYPE = "point";

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(GeoShapeFieldMapper.class);

    private static Builder builder(FieldMapper in) {
        return ((PointFieldMapper)in).builder;
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
                () -> null).acceptsNull();
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, hasDocValues, stored, ignoreMalformed, ignoreZValue, nullValue, meta);
        }

        private static CartesianPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
            CartesianPoint point = new CartesianPoint();
            CartesianPoint.parsePoint(nullValue, point, ignoreZValue);
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
        public FieldMapper build(ContentPath contentPath) {
            if (multiFieldsBuilder.hasMultiFields()) {
                DEPRECATION_LOGGER.deprecate(
                    DeprecationCategory.MAPPINGS,
                    "point_multifields",
                    "Adding multifields to [point] mappers has no effect and will be forbidden in future"
                );
            }
            CartesianPointParser parser
                = new CartesianPointParser(
                name,
                CartesianPoint::new,
                (p, point) -> {
                    CartesianPoint.parsePoint(p, point, ignoreZValue.get().value());
                    return point;
                },
                nullValue.get(),
                ignoreZValue.get().value(), ignoreMalformed.get().value());
            PointFieldType ft
                = new PointFieldType(buildFullName(contentPath), indexed.get(), stored.get(), hasDocValues.get(), parser, meta.get());
            return new PointFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath),
                copyTo.build(), parser, this);
        }

    }

    public static TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;

    public PointFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                            MultiFields multiFields, CopyTo copyTo,
                            CartesianPointParser parser, Builder builder) {
        super(simpleName, mappedFieldType, multiFields,
            builder.ignoreMalformed.get(), builder.ignoreZValue.get(), builder.nullValue.get(), copyTo, parser);
        this.builder = builder;
    }

    @Override
    protected void index(DocumentParserContext context, CartesianPoint point) throws IOException {
        if (fieldType().isSearchable()) {
            context.doc().add(new XYPointField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        }
        if (fieldType().hasDocValues()) {
            context.doc().add(new XYDocValuesField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        } else if (fieldType().isStored() || fieldType().isSearchable()) {
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
        return new Builder(simpleName(), builder.ignoreMalformed.getDefaultValue().value()).init(this);
    }

    public static class PointFieldType extends AbstractGeometryFieldType<CartesianPoint> implements ShapeQueryable {

        private final ShapeQueryPointProcessor queryProcessor;

        private PointFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues,
                               CartesianPointParser parser, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, parser, meta);
            this.queryProcessor = new ShapeQueryPointProcessor();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, SearchExecutionContext context) {
            return queryProcessor.shapeQuery(shape, fieldName, relation, context);
        }

        @Override
        protected Function<List<CartesianPoint>, List<Object>> getFormatter(String format) {
           return GeometryFormatterFactory.getFormatter(format, p -> new Point(p.getX(), p.getY()));
        }
    }

    /** CartesianPoint parser implementation */
    private static class CartesianPointParser extends PointParser<CartesianPoint> {

        CartesianPointParser(String field,
                             Supplier<CartesianPoint> pointSupplier,
                             CheckedBiFunction<XContentParser, CartesianPoint, CartesianPoint, IOException> objectParser,
                             CartesianPoint nullValue,
                             boolean ignoreZValue,
                             boolean ignoreMalformed) {
            super(field, pointSupplier, objectParser, nullValue, ignoreZValue, ignoreMalformed);
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
        protected void reset(CartesianPoint in, double x, double y) {
            in.reset(x, y);
        }
    }
}
