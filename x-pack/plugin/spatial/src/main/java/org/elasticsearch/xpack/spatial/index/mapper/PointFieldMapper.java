/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
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
import org.elasticsearch.index.mapper.BlockDocValuesReader;
import org.elasticsearch.index.mapper.BlockLoader;
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

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.DOC_VALUES;

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
                /*
                 * We have no plans to fail on multifields because it isn't worth breaking
                 * even the tiny fraction of users.
                 */
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.MAPPINGS,
                    "point_multifields",
                    "Adding multifields to [point] mappers has no effect"
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
                context.isSourceSynthetic(),
                meta.get()
            );
            return new PointFieldMapper(leafName(), ft, builderParams(this, context), parser, this);
        }

    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, IGNORE_MALFORMED_SETTING.get(c.getSettings())));

    private final Builder builder;

    public PointFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        CartesianPointParser parser,
        Builder builder
    ) {
        super(
            simpleName,
            mappedFieldType,
            builderParams,
            builder.ignoreMalformed.get(),
            builder.ignoreZValue.get(),
            builder.nullValue.get(),
            parser
        );
        this.builder = builder;
    }

    @Override
    protected void index(DocumentParserContext context, CartesianPoint point) {
        final boolean indexed = fieldType().isIndexed();
        final boolean hasDocValues = fieldType().hasDocValues();
        final boolean store = fieldType().isStored();
        if (indexed && hasDocValues) {
            context.doc().add(new XYFieldWithDocValues(fieldType().name(), (float) point.getX(), (float) point.getY()));
        } else if (hasDocValues) {
            context.doc().add(new XYDocValuesField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        } else if (indexed) {
            context.doc().add(new XYPointField(fieldType().name(), (float) point.getX(), (float) point.getY()));
        }
        if (store) {
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
        private final boolean isSyntheticSource;

        private PointFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            CartesianPointParser parser,
            CartesianPoint nullValue,
            boolean isSyntheticSource,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, parser, nullValue, meta);
            this.isSyntheticSource = isSyntheticSource;
        }

        // only used in test
        public PointFieldType(String name) {
            this(name, true, false, true, null, null, false, Collections.emptyMap());
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

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (blContext.fieldExtractPreference() == DOC_VALUES && hasDocValues()) {
                return new BlockDocValuesReader.LongsBlockLoader(name());
            }

            // Multi fields don't have fallback synthetic source.s
            if (isSyntheticSource && blContext.parentField(name()) == null) {
                return blockLoaderFromFallbackSyntheticSource(blContext);
            }

            return blockLoaderFromSource(blContext);
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

    /**
     * Utility class that allows adding index and doc values in one field
     */
    static class XYFieldWithDocValues extends Field {

        private static final FieldType TYPE = new FieldType();

        static {
            TYPE.setDimensions(2, Integer.BYTES);
            TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
            TYPE.freeze();
        }

        // holds the doc value value.
        private final long docValue;

        XYFieldWithDocValues(String name, float x, float y) {
            super(name, TYPE);
            final byte[] bytes;
            if (fieldsData == null) {
                bytes = new byte[8];
                fieldsData = new BytesRef(bytes);
            } else {
                bytes = ((BytesRef) fieldsData).bytes;
            }

            int xEncoded = XYEncodingUtils.encode(x);
            int yEncoded = XYEncodingUtils.encode(y);
            NumericUtils.intToSortableBytes(xEncoded, bytes, 0);
            NumericUtils.intToSortableBytes(yEncoded, bytes, 4);

            docValue = (((long) xEncoded) << 32) | (yEncoded & 0xFFFFFFFFL);
        }

        @Override
        public Number numericValue() {
            return docValue;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(getClass().getSimpleName());
            result.append(" <");
            result.append(name);
            result.append(':');

            byte[] bytes = ((BytesRef) fieldsData).bytes;
            result.append(XYEncodingUtils.decode(bytes, 0));
            result.append(',');
            result.append(XYEncodingUtils.decode(bytes, Integer.BYTES));

            result.append('>');
            return result.toString();
        }
    }
}
