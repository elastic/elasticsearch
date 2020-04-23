/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryPointProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;


/**
 * Field Mapper for point type.
 *
 * Uses lucene 8 XYPoint encoding
 */
public class PointFieldMapper extends AbstractGeometryFieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "point";

    public static class Names extends AbstractGeometryFieldMapper.Names {
        public static final ParseField NULL_VALUE = new ParseField("null_value");
    }

    public static class Builder extends AbstractGeometryFieldMapper.Builder<Builder, PointFieldMapper> {
        public Builder(String name) {
            super(name, new PointFieldType(), new PointFieldType());
            builder = this;
        }

        public PointFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                      MappedFieldType defaultFieldType, Settings indexSettings,
                                      MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                      CopyTo copyTo) {
            setupFieldType(context);
            return new PointFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                ignoreMalformed, ignoreZValue(context), copyTo);
        }

        @Override
        public PointFieldType fieldType() {
            return (PointFieldType)fieldType;
        }

        @Override
        public PointFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);

            fieldType().setGeometryQueryBuilder(new ShapeQueryPointProcessor());
        }
    }

    public static class TypeParser extends AbstractGeometryFieldMapper.TypeParser<Builder> {
        @Override
        protected Builder newBuilder(String name, Map<String, Object> params) {
            return new PointFieldMapper.Builder(name);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Builder parse(String name, Map<String, Object> node, Map<String, Object> params, ParserContext parserContext) {
            Builder builder = super.parse(name, node, params, parserContext);
            parseField(builder, name, node, parserContext);
            Object nullValue = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (Names.NULL_VALUE.match(propName, LoggingDeprecationHandler.INSTANCE)) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    nullValue = propNode;
                    iterator.remove();
                }
            }

            if (nullValue != null) {
                boolean ignoreMalformed = builder.ignoreMalformed().value();
                boolean ignoreZValue = builder.ignoreZValue().value();
                CartesianPoint point = CartesianPoint.parsePoint(nullValue, ignoreZValue);
                if (ignoreMalformed == false) {
                    if (Float.isFinite(point.getX()) == false) {
                        throw new IllegalArgumentException("illegal x value [" + point.getX() + "]");
                    }
                    if (Float.isFinite(point.getY()) == false) {
                        throw new IllegalArgumentException("illegal y value [" + point.getY() + "]");
                    }
                }
                builder.nullValue(point);
            }
            return builder;
        }
    }

    public PointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                            Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                            Explicit<Boolean> ignoreZValue, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, ignoreMalformed, ignoreZValue, multiFields, copyTo);
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class PointFieldType extends AbstractGeometryFieldType {
        public PointFieldType() {
            super();
            setHasDocValues(true);
            setDimensions(2, Integer.BYTES);
        }

        PointFieldType(PointFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new PointFieldType(this);
        }
    }

    protected void parse(ParseContext context, CartesianPoint point) throws IOException {

        if (fieldType().indexOptions() != IndexOptions.NONE) {
            context.doc().add(new XYPointField(fieldType().name(), point.getX(), point.getY()));
        }
        if (fieldType().stored()) {
            context.doc().add(new StoredField(fieldType().name(), point.toString()));
        }
        if (fieldType.hasDocValues()) {
            context.doc().add(new XYDocValuesField(fieldType().name(), point.getX(), point.getY()));
        } else if (fieldType().stored() || fieldType().indexOptions() != IndexOptions.NONE) {
            List<IndexableField> fields = new ArrayList<>(1);
            createFieldNamesField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        }
        // if the mapping contains multi-fields then throw an error?
        if (multiFields.iterator().hasNext()) {
            throw new ElasticsearchParseException("[{}] field type does not accept multi-fields", CONTENT_TYPE);
        }
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());

        try {
            CartesianPoint sparse = context.parseExternalValue(CartesianPoint.class);

            if (sparse != null) {
                parse(context, sparse);
            } else {
                sparse = new CartesianPoint();
                XContentParser.Token token = context.parser().currentToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    token = context.parser().nextToken();
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        float x = context.parser().floatValue();
                        context.parser().nextToken();
                        float y = context.parser().floatValue();
                        token = context.parser().nextToken();
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            CartesianPoint.assertZValue(ignoreZValue.value(), context.parser().floatValue());
                        } else if (token != XContentParser.Token.END_ARRAY) {
                            throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions", CONTENT_TYPE);
                        }
                        parse(context, sparse.reset(x, y));
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            parsePointIgnoringMalformed(context, sparse);
                            token = context.parser().nextToken();
                        }
                    }
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (fieldType.nullValue() != null) {
                        parse(context, (CartesianPoint) fieldType.nullValue());
                    }
                } else {
                    parsePointIgnoringMalformed(context, sparse);
                }
            }
        } catch (Exception ex) {
            throw new MapperParsingException("failed to parse field [{}] of type [{}]", ex, fieldType().name(), fieldType().typeName());
        }

        context.path().remove();
    }

    /**
     * Parses point represented as an object or an array, ignores malformed points if needed
     */
    private void parsePointIgnoringMalformed(ParseContext context, CartesianPoint sparse) throws IOException {
        try {
            parse(context, CartesianPoint.parsePoint(context.parser(), sparse, ignoreZValue().value()));
        } catch (ElasticsearchParseException e) {
            if (ignoreMalformed.value() == false) {
                throw e;
            }
            context.addIgnoredField(fieldType.name());
        }
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field(Names.NULL_VALUE.getPreferredName(), fieldType().nullValue());
        }
    }
}
