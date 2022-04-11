/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static org.elasticsearch.xpack.sql.proto.content.ParserUtils.location;

/**
 * NB: cloned from the class with the same name in ES XContent.
 */
public class ObjectParser<Value, Context> extends AbstractObjectParser<Value, Context> {

    private final Map<String, FieldParser> fieldParsers = new HashMap<>();
    private final List<String[]> requiredFieldSets = new ArrayList<>();

    private final String name;
    private final Function<Context, Value> valueBuilder;
    private final UnknownFieldParser<Value, Context> unknownFieldParser;

    public interface Parser<Value, Context> {
        void parse(JsonParser parser, Value value, Context context) throws IOException;
    }

    private interface UnknownFieldParser<Value, Context> {
        void acceptUnknownField(
            ObjectParser<Value, Context> objectParser,
            String field,
            JsonLocation location,
            JsonParser parser,
            Value value,
            Context context
        ) throws IOException;
    }

    private class FieldParser {
        private final Parser<Value, Context> parser;
        private final String field;
        private final ValueType type;

        FieldParser(Parser<Value, Context> parser, String field, ValueType type) {
            this.parser = parser;
            this.field = field;
            this.type = type;
        }

        void assertSupports(JsonParser jsonParser, String currentFieldName) {
            JsonToken currentToken = jsonParser.currentToken();
            if (currentFieldName.equals(field) == false) {
                throw new ParseException(location(jsonParser), "[" + name + "] doesn't accept: " + currentFieldName);
            }
            if (type.supportedTokens().contains(currentToken) == false) {
                throw new ParseException(
                    location(jsonParser),
                    "[" + name + "] " + currentFieldName + " doesn't support values of type: " + currentToken
                );
            }
        }
    }

    private static <Value, Context> UnknownFieldParser<Value, Context> ignoreUnknown() {
        return (op, f, l, p, v, c) -> p.skipChildren();
    }

    private static <Value, Context> UnknownFieldParser<Value, Context> errorOnUnknown() {
        return (op, f, l, p, v, c) -> { throw new ParseException(location(l), "[" + op.name + "] unknown field [" + f + "]"); };
    }

    public ObjectParser(String name, boolean ignoreUnknownFields, Supplier<Value> valueSupplier) {
        this.name = name;
        this.unknownFieldParser = ignoreUnknownFields ? ignoreUnknown() : errorOnUnknown();
        this.valueBuilder = wrapValueSupplier(valueSupplier);
    }

    private static <C, V> Function<C, V> wrapValueSupplier(Supplier<V> valueSupplier) {
        return valueSupplier == null ? c -> { throw new NullPointerException(); } : c -> valueSupplier.get();
    }

    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, String field, ValueType type) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        declareField((p, v, c) -> consumer.accept(v, parser.parse(p, c)), field, type);
    }

    public void declareField(Parser<Value, Context> p, String parseField, ValueType type) {
        fieldParsers.put(parseField, new FieldParser(p, parseField, type));
    }

    public void declareRequiredFieldSet(String... requiredSet) {
        if (requiredSet.length == 0) {
            return;
        }
        this.requiredFieldSets.add(requiredSet);
    }

    public String getName() {
        return name;
    }

    @Override
    public Value parse(JsonParser parser, Context context) throws IOException {
        return parse(parser, valueBuilder.apply(context), context);
    }

    Value parse(JsonParser parser, Value value, Context context) throws IOException {
        JsonToken token;

        if (parser.currentToken() != START_OBJECT) {
            token = parser.nextToken();
            if (token != START_OBJECT) {
                throwExpectedStartObject(parser, token);
            }
        }

        FieldParser fieldParser = null;
        String currentFieldName = null;
        JsonLocation currentPosition = null;

        final List<String[]> requiredFields = this.requiredFieldSets.isEmpty() ? null : new ArrayList<>(this.requiredFieldSets);

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                currentPosition = parser.getTokenLocation();
                fieldParser = fieldParsers.get(currentFieldName);
            } else {
                if (currentFieldName == null) {
                    throwNoFieldFound(parser);
                }
                if (fieldParser == null) {
                    unknownFieldParser.acceptUnknownField(this, currentFieldName, currentPosition, parser, value, context);
                } else {
                    fieldParser.assertSupports(parser, currentFieldName);

                    if (requiredFields != null) {
                        // Check to see if this field is a required field, if it is we can
                        // remove the entry as the requirement is satisfied
                        maybeMarkRequiredField(currentFieldName, requiredFields);
                    }

                    parseSub(parser, fieldParser, currentFieldName, value, context);
                }
                fieldParser = null;
            }
        }
        if (requiredFields != null && requiredFields.isEmpty() == false) {
            throwMissingRequiredFields(requiredFields);
        }
        return value;
    }

    private void maybeMarkRequiredField(String currentFieldName, List<String[]> requiredFields) {
        Iterator<String[]> iter = requiredFields.iterator();
        while (iter.hasNext()) {
            String[] requiredFieldNames = iter.next();
            for (String field : requiredFieldNames) {
                if (field.equals(currentFieldName)) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    private void parseSub(JsonParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) {
        final JsonToken token = parser.currentToken();
        switch (token) {
            case START_OBJECT:
                parseValue(parser, fieldParser, currentFieldName, value, context);

                if (parser.currentToken() != JsonToken.END_OBJECT) {
                    throwMustEndOn(parser, currentFieldName, JsonToken.END_OBJECT);
                }
                break;
            case START_ARRAY:
                parseArray(parser, fieldParser, currentFieldName, value, context);

                if (parser.currentToken() != JsonToken.END_ARRAY) {
                    throwMustEndOn(parser, currentFieldName, JsonToken.END_ARRAY);
                }
                break;
            case END_OBJECT:
            case END_ARRAY:
            case FIELD_NAME:
                throw throwUnexpectedToken(parser, token);
            case VALUE_STRING:
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_EMBEDDED_OBJECT:
            case VALUE_NULL:
                parseValue(parser, fieldParser, currentFieldName, value, context);
        }
    }

    private void parseArray(JsonParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) {
        throwExpectedStartArray(parser);
        parseValue(parser, fieldParser, currentFieldName, value, context);
    }

    private void parseValue(JsonParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) {
        try {
            fieldParser.parser.parse(parser, value, context);
        } catch (Exception ex) {
            throwFailedToParse(parser, currentFieldName, ex);
        }
    }

    private void throwMustEndOn(JsonParser parser, String currentFieldName, JsonToken token) {
        throw new ParseException(location(parser), "parser for [" + currentFieldName + "] did not end on " + token);
    }

    private ParseException throwUnexpectedToken(JsonParser parser, JsonToken token) {
        return new ParseException(location(parser), "[" + name + "]" + token + " is unexpected");
    }

    private void throwExpectedStartObject(JsonParser parser, JsonToken token) {
        throw new ParseException(location(parser), "[" + name + "] Expected START_OBJECT but was: " + token);
    }

    private void throwExpectedStartArray(JsonParser parser) {
        if (parser.currentToken() != START_ARRAY) {
            throw new ParseException(location(parser), "[" + name + "] Expected START_ARRAY but was: " + parser.currentToken());
        }
    }

    private void throwNoFieldFound(JsonParser parser) {
        throw new ParseException(location(parser), "[" + name + "] no field found");
    }

    private void throwFailedToParse(JsonParser parser, String currentFieldName, Exception ex) {
        throw new ParseException(location(parser), "[" + name + "] failed to parse field [" + currentFieldName + "]", ex);
    }

    private void throwMissingRequiredFields(List<String[]> requiredFields) {
        final StringBuilder message = new StringBuilder();
        for (String[] fields : requiredFields) {
            message.append("Required one of fields ").append(Arrays.toString(fields)).append(", but none were specified. ");
        }
        throw new ParseException(message.toString());
    }
}
