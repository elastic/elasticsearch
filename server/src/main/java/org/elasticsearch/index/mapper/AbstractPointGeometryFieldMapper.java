/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/** Base class for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {

    public static <T> Parameter<T> nullValueParam(
        Function<FieldMapper, T> initializer,
        TriFunction<String, MappingParserContext, Object, T> parser,
        Supplier<T> def,
        Serializer<T> serializer
    ) {
        return new Parameter<T>("null_value", false, def, parser, initializer, serializer, Objects::toString);
    }

    protected final T nullValue;

    protected AbstractPointGeometryFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        Explicit<Boolean> ignoreMalformed,
        Explicit<Boolean> ignoreZValue,
        T nullValue,
        Parser<T> parser
    ) {
        super(simpleName, mappedFieldType, builderParams, ignoreMalformed, ignoreZValue, parser);
        this.nullValue = nullValue;
    }

    public T getNullValue() {
        return nullValue;
    }

    /** A base parser implementation for point formats */
    protected abstract static class PointParser<T> extends Parser<T> {
        protected final String field;
        protected final CheckedFunction<XContentParser, T, IOException> objectParser;
        private final T nullValue;
        private final boolean ignoreZValue;
        protected final boolean ignoreMalformed;
        private final boolean allowMultipleValues;

        protected PointParser(
            String field,
            CheckedFunction<XContentParser, T, IOException> objectParser,
            T nullValue,
            boolean ignoreZValue,
            boolean ignoreMalformed,
            boolean allowMultipleValues
        ) {
            this.field = field;
            this.objectParser = objectParser;
            this.nullValue = nullValue == null ? null : validate(nullValue);
            this.ignoreZValue = ignoreZValue;
            this.ignoreMalformed = ignoreMalformed;
            this.allowMultipleValues = allowMultipleValues;
        }

        protected abstract T validate(T in);

        protected abstract T createPoint(double x, double y);

        @Override
        public void parse(XContentParser parser, CheckedConsumer<T, IOException> consumer, MalformedValueHandler malformedHandler)
            throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    double x = parser.doubleValue();
                    parser.nextToken();
                    double y = parser.doubleValue();
                    token = parser.nextToken();
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (ignoreZValue == false) {
                            throw new ElasticsearchParseException(
                                "Exception parsing coordinates: found Z value [{}] but [ignore_z_value] " + "parameter is [{}]",
                                parser.doubleValue(),
                                ignoreZValue
                            );
                        }
                    } else if (token != XContentParser.Token.END_ARRAY) {
                        throw new ElasticsearchParseException("field type does not accept > 3 dimensions");
                    }

                    T point = createPoint(x, y);
                    consumer.accept(validate(point));
                } else {
                    int count = 0;
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (allowMultipleValues == false && ++count > 1) {
                            throw new ElasticsearchParseException("field type for [{}] does not accept more than single value", field);
                        }
                        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                            if (nullValue != null) {
                                consumer.accept(nullValue);
                            }
                        } else {
                            parseAndConsumeFromObject(parser, consumer, malformedHandler);
                        }
                        token = parser.nextToken();
                    }
                }
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    consumer.accept(nullValue);
                }
            } else {
                parseAndConsumeFromObject(parser, consumer, malformedHandler);
            }
        }

        protected void parseAndConsumeFromObject(
            XContentParser parser,
            CheckedConsumer<T, IOException> consumer,
            MalformedValueHandler malformedHandler
        ) throws IOException {
            try {
                T point = objectParser.apply(parser);
                consumer.accept(validate(point));
            } catch (Exception e) {
                malformedHandler.notify(e);
            }
        }
    }

    public abstract static class AbstractPointFieldType<T extends SpatialPoint> extends AbstractGeometryFieldType<T> {
        protected AbstractPointFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            Parser<T> geometryParser,
            T nullValue,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, geometryParser, nullValue, meta);
        }

        @Override
        protected Object nullValueAsSource(T nullValue) {
            return nullValue == null ? null : nullValue.toWKT();
        }
    }
}
