/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Base class for for spatial fields that only support indexing points */
public abstract class AbstractPointGeometryFieldMapper<T> extends AbstractGeometryFieldMapper<T> {

    public static <T> Parameter<T> nullValueParam(Function<FieldMapper, T> initializer,
                                                  TriFunction<String, MappingParserContext, Object, T> parser,
                                                  Supplier<T> def) {
        return new Parameter<T>("null_value", false, def, parser, initializer);
    }

    protected final T nullValue;

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                               Explicit<Boolean> ignoreZValue, T nullValue, CopyTo copyTo,
                                               Parser<T> parser) {
        super(simpleName, mappedFieldType, ignoreMalformed, ignoreZValue, multiFields, copyTo, parser);
        this.nullValue = nullValue;
    }

    protected AbstractPointGeometryFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                               MultiFields multiFields, CopyTo copyTo,
                                               Parser<T> parser, String onScriptError) {
        super(simpleName, mappedFieldType, multiFields, copyTo, parser, onScriptError);
        this.nullValue = null;
    }

    public T getNullValue() {
        return nullValue;
    }

    /** A base parser implementation for point formats */
    protected abstract static class PointParser<T> extends Parser<T> {
        protected final String field;
        private final Supplier<T> pointSupplier;
        private final CheckedBiFunction<XContentParser, T, T, IOException> objectParser;
        private final T nullValue;
        private final boolean ignoreZValue;
        protected final boolean ignoreMalformed;

        protected PointParser(String field,
                              Supplier<T> pointSupplier,
                              CheckedBiFunction<XContentParser, T, T, IOException> objectParser,
                              T nullValue,
                              boolean ignoreZValue,
                              boolean ignoreMalformed) {
            this.field = field;
            this.pointSupplier = pointSupplier;
            this.objectParser = objectParser;
            this.nullValue = nullValue == null ? null : validate(nullValue);
            this.ignoreZValue = ignoreZValue;
            this.ignoreMalformed = ignoreMalformed;
        }

        protected abstract T validate(T in);

        protected abstract void reset(T in, double x, double y);

        @Override
        public void parse(
            XContentParser parser,
            CheckedConsumer<T, IOException> consumer,
            Consumer<Exception> onMalformed
        ) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                XContentParser.Token token = parser.nextToken();
                T point = pointSupplier.get();
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    double x = parser.doubleValue();
                    parser.nextToken();
                    double y = parser.doubleValue();
                    token = parser.nextToken();
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (ignoreZValue == false) {
                            throw new ElasticsearchParseException("Exception parsing coordinates: found Z value [{}] but [ignore_z_value] "
                                + "parameter is [{}]", parser.doubleValue(), ignoreZValue);
                        }
                    } else if (token != XContentParser.Token.END_ARRAY) {
                        throw new ElasticsearchParseException("field type does not accept > 3 dimensions");
                    }

                    reset(point, x, y);
                    consumer.accept(validate(point));
                } else {
                    while (token != XContentParser.Token.END_ARRAY) {
                        parseAndConsumeFromObject(parser, point, consumer, onMalformed);
                        point = pointSupplier.get();
                        token = parser.nextToken();
                    }
                }
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    consumer.accept(nullValue);
                }
            } else {
                parseAndConsumeFromObject(parser, pointSupplier.get(), consumer, onMalformed);
            }
        }

        private void parseAndConsumeFromObject(
            XContentParser parser,
            T point,
            CheckedConsumer<T, IOException> consumer,
            Consumer<Exception> onMalformed
        ) {
            try {
                point = objectParser.apply(parser, point);
                consumer.accept(validate(point));
            } catch (Exception e) {
                onMalformed.accept(e);
            }
        }
    }
}
