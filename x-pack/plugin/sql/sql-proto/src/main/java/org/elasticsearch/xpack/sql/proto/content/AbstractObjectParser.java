/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.xpack.sql.proto.core.CheckedFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_EMBEDDED_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.BOOLEAN;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.INT;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.OBJECT;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.OBJECT_ARRAY;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.STRING;
import static org.elasticsearch.xpack.sql.proto.content.ParserUtils.location;

/**
 * NB: cloned from the class with the same name in ES XContent.
 */
public abstract class AbstractObjectParser<Value, Context> implements BiFunction<JsonParser, Context, Value> {

    protected String name;

    public interface ContextParser<Context, T> {
        T parse(JsonParser p, Context c) throws IOException;
    }

    public void declareBoolean(BiConsumer<Value, Boolean> consumer, String field) {
        declareField(consumer, ParserUtils::booleanValue, field, BOOLEAN);
    }

    public void declareInt(BiConsumer<Value, Integer> consumer, String field) {
        declareField(consumer, ParserUtils::intValue, field, INT);
    }

    public void declareString(BiConsumer<Value, String> consumer, String field) {
        declareField(consumer, ParserUtils::text, field, STRING);
    }

    public <T> void declareObject(BiConsumer<Value, T> consumer, ContextParser<Context, T> objectParser, String field) {
        declareField(consumer, objectParser, field, OBJECT);
    }

    public <T> void declareObjectArray(BiConsumer<Value, List<T>> consumer, ContextParser<Context, T> objectParser, String field) {
        declareFieldArray(consumer, objectParser, field, OBJECT_ARRAY);
    }

    public <T> void declareFieldArray(
        BiConsumer<Value, List<T>> consumer,
        ContextParser<Context, T> itemParser,
        String field,
        ValueType type
    ) {
        declareField(consumer, (p, c) -> parseArray(p, () -> itemParser.parse(p, c)), field, type);
    }

    public <T> void declareField(
        BiConsumer<Value, T> consumer,
        CheckedFunction<JsonParser, T, IOException> parser,
        String parseField,
        ValueType type
    ) {
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        declareField(consumer, (p, c) -> parser.apply(p), parseField, type);
    }

    public abstract <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, String field, ValueType type);

    @Override
    public Value apply(JsonParser parser, Context context) {
        try {
            return parse(parser, context);
        } catch (IOException e) {
            throw new ParseException(location(parser), "[" + name + "] failed to parse object", e);
        }
    }

    abstract Value parse(JsonParser parser, Context context) throws IOException;

    public enum ValueType {
        STRING(VALUE_STRING),
        STRING_OR_NULL(VALUE_STRING, VALUE_NULL),
        FLOAT(VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING),
        FLOAT_OR_NULL(VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING, VALUE_NULL),
        DOUBLE(VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING),
        DOUBLE_OR_NULL(VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING, VALUE_NULL),
        LONG(VALUE_NUMBER_INT, VALUE_STRING),
        LONG_OR_NULL(VALUE_NUMBER_INT, VALUE_STRING, VALUE_NULL),
        INT(VALUE_NUMBER_INT, VALUE_STRING),
        INT_OR_NULL(VALUE_NUMBER_INT, VALUE_STRING, VALUE_NULL),
        BOOLEAN(VALUE_FALSE, VALUE_TRUE, VALUE_STRING),
        BOOLEAN_OR_NULL(VALUE_FALSE, VALUE_TRUE, VALUE_STRING, VALUE_NULL),
        STRING_ARRAY(START_ARRAY, VALUE_STRING),
        FLOAT_ARRAY(START_ARRAY, VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING),
        DOUBLE_ARRAY(START_ARRAY, VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING),
        LONG_ARRAY(START_ARRAY, VALUE_NUMBER_INT, VALUE_STRING),
        INT_ARRAY(START_ARRAY, VALUE_NUMBER_INT, VALUE_STRING),
        BOOLEAN_ARRAY(START_ARRAY, VALUE_FALSE, VALUE_TRUE),
        OBJECT(START_OBJECT),
        OBJECT_OR_NULL(START_OBJECT, VALUE_NULL),
        OBJECT_ARRAY(START_OBJECT, START_ARRAY),
        OBJECT_ARRAY_OR_NULL(START_OBJECT, START_ARRAY, VALUE_NULL),
        OBJECT_OR_BOOLEAN(START_OBJECT, VALUE_FALSE, VALUE_TRUE),
        OBJECT_OR_STRING(START_OBJECT, VALUE_STRING),
        OBJECT_OR_LONG(START_OBJECT, VALUE_NUMBER_INT),
        OBJECT_ARRAY_BOOLEAN_OR_STRING(START_OBJECT, START_ARRAY, VALUE_FALSE, VALUE_TRUE, VALUE_STRING),
        OBJECT_ARRAY_OR_STRING(START_OBJECT, START_ARRAY, VALUE_STRING),
        OBJECT_ARRAY_STRING_OR_NUMBER(START_OBJECT, START_ARRAY, VALUE_STRING, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT),
        VALUE(VALUE_FALSE, VALUE_TRUE, VALUE_NULL, VALUE_EMBEDDED_OBJECT, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT, VALUE_STRING),
        VALUE_OBJECT_ARRAY(
            VALUE_FALSE,
            VALUE_TRUE,
            VALUE_NULL,
            VALUE_EMBEDDED_OBJECT,
            VALUE_NUMBER_INT,
            VALUE_NUMBER_FLOAT,
            VALUE_STRING,
            START_OBJECT,
            START_ARRAY
        ),
        VALUE_ARRAY(VALUE_FALSE, VALUE_TRUE, VALUE_NULL, VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_STRING, START_ARRAY);

        private final EnumSet<JsonToken> tokens;

        ValueType(JsonToken first, JsonToken... rest) {
            this.tokens = EnumSet.of(first, rest);
        }

        public EnumSet<JsonToken> supportedTokens() {
            return this.tokens;
        }
    }

    private interface IOSupplier<T> {
        T get() throws IOException;
    }

    private static <T> List<T> parseArray(JsonParser parser, IOSupplier<T> supplier) throws IOException {
        List<T> list = new ArrayList<>();
        JsonToken token = parser.currentToken();
        if (token.isScalarValue() || token == JsonToken.VALUE_NULL || token == JsonToken.START_OBJECT) {
            list.add(supplier.get()); // single value
        } else {
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                token = parser.currentToken();
                if (token.isScalarValue() || token == JsonToken.VALUE_NULL || token == JsonToken.START_OBJECT) {
                    list.add(supplier.get());
                } else {
                    throw new IllegalStateException("expected value but got [" + token + "]");
                }
            }
        }
        return list;
    }
}
