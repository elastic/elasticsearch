/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A declarative Object parser to parse any kind of XContent structures into existing object with setters.
 * The Parser is designed to be declarative and stateless. A single parser is defined for one object level, nested
 * elements can be added via {@link #declareObject(BiConsumer, BiFunction, ParseField)} which is commonly done by
 * declaring yet another instance of {@link ObjectParser}. Instances of {@link ObjectParser} are thread-safe and can be
 * re-used across parsing operations. It's recommended to use the high level declare methods like {@link #declareString(BiConsumer, ParseField)}
 * instead of {@link #declareField} which can be used to implement exceptional parsing operations not covered by the high level methods.
 */
public final class ObjectParser<Value, Context> implements BiFunction<XContentParser, Context, Value> {

    private final String name;
    private final Supplier<Value> valueSupplier;

    /**
     * Creates a new ObjectParser instance with a name. This name is used to reference the parser in exceptions and messages.
     */
    public ObjectParser(String name) {
        this(name, null);
    }

    /**
     * Creates a new ObjectParser instance which a name.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param valueSupplier a supplier that creates a new Value instance used when the parser is used as an inner object parser.
     */
    public ObjectParser(String name, Supplier<Value> valueSupplier) {
        this.name = name;
        this.valueSupplier = valueSupplier;
    }

    /**
     * Parses a Value from the given {@link XContentParser}
     * @param parser the parser to build a value from
     * @return a new value instance drawn from the provided value supplier on {@link #ObjectParser(String, Supplier)}
     * @throws IOException if an IOException occurs.
     */
    public Value parse(XContentParser parser) throws IOException {
        if (valueSupplier == null) {
            throw new NullPointerException("valueSupplier is not set");
        }
        return parse(parser, valueSupplier.get(), null);
    }

    /**
     * Parses a Value from the given {@link XContentParser}
     * @param parser the parser to build a value from
     * @param value the value to fill from the parser
     * @return the parsed value
     * @throws IOException if an IOException occurs.
     */
    public Value parse(XContentParser parser, Value value) throws IOException {
        return parse(parser, value, null);
    }

    /**
     * Parses a Value from the given {@link XContentParser}
     * @param parser the parser to build a value from
     * @param value the value to fill from the parser
     * @param context an optional context that is passed along to all declared field parsers
     * @return the parsed value
     * @throws IOException if an IOException occurs.
     */
    public Value parse(XContentParser parser, Value value, Context context) throws IOException {
        XContentParser.Token token;
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            token = parser.currentToken();
        } else {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("[" + name  + "] Expected START_OBJECT but was: " + token);
            }
        }

        FieldParser<Value> fieldParser = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                fieldParser = getParser(currentFieldName);
            } else {
                if (currentFieldName == null) {
                    throw new IllegalStateException("[" + name  + "] no field found");
                }
                assert fieldParser != null;
                fieldParser.assertSupports(name, token, currentFieldName, parser.getParseFieldMatcher());
                parseSub(parser, fieldParser, currentFieldName, value, context);
                fieldParser = null;
            }
        }
        return value;
    }

    private void parseArray(XContentParser parser, FieldParser<Value> fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "Token was: " + parser.currentToken();
        parseValue(parser, fieldParser, currentFieldName, value, context);
    }

    private void parseValue(XContentParser parser, FieldParser<Value> fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        try {
            fieldParser.parser.parse(parser, value, context);
        } catch (Exception ex) {
            throw new ParsingException(parser.getTokenLocation(), "[" + name  + "] failed to parse field [" + currentFieldName + "]", ex);
        }
    }

    private void parseSub(XContentParser parser, FieldParser<Value> fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        final XContentParser.Token token = parser.currentToken();
        switch (token) {
            case START_OBJECT:
                parseValue(parser, fieldParser, currentFieldName, value, context);
                break;
            case START_ARRAY:
                parseArray(parser, fieldParser, currentFieldName, value, context);
                break;
            case END_OBJECT:
            case END_ARRAY:
            case FIELD_NAME:
                throw new IllegalStateException("[" + name  + "]" + token + " is unexpected");
            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_BOOLEAN:
            case VALUE_EMBEDDED_OBJECT:
            case VALUE_NULL:
                parseValue(parser, fieldParser, currentFieldName, value, context);
        }
    }

    protected FieldParser getParser(String fieldName) {
        FieldParser<Value> parser = fieldParserMap.get(fieldName);
        if (parser == null) {
            throw new IllegalArgumentException("[" + name  + "] unknown field [" + fieldName + "], parser not found");
        }
        return parser;
    }

    @Override
    public Value apply(XContentParser parser, Context context) {
        if (valueSupplier == null) {
            throw new NullPointerException("valueSupplier is not set");
        }
        try {
            return parse(parser, valueSupplier.get(), context);
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "[" + name  + "] failed to parse object", e);
        }
    }

    public interface Parser<Value, Context> {
        void parse(XContentParser parser, Value value, Context context) throws IOException;
    }

    private interface IOSupplier<T> {
        T get() throws IOException;
    }

    private final Map<String, FieldParser> fieldParserMap = new HashMap<>();

    public void declareField(Parser<Value, Context> p, ParseField parseField, ValueType type) {
        FieldParser fieldParser = new FieldParser(p, type.supportedTokens(), parseField, type);
        for (String fieldValue : parseField.getAllNamesIncludedDeprecated()) {
            fieldParserMap.putIfAbsent(fieldValue, fieldParser);
        }
    }

    public void declareStringArray(BiConsumer<Value, List<String>> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, parseArray(p, p::text)), field, ValueType.STRING_ARRAY);
    }

    public void declareDoubleArray(BiConsumer<Value, List<Double>> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, parseArray(p, p::doubleValue)), field, ValueType.DOUBLE_ARRAY);
    }

    public void declareFloatArray(BiConsumer<Value, List<Float>> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, parseArray(p, p::floatValue)), field, ValueType.FLOAT_ARRAY);
    }

    public void declareLongArray(BiConsumer<Value, List<Long>> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, parseArray(p, p::longValue)), field, ValueType.LONG_ARRAY);
    }

    public void declareIntArray(BiConsumer<Value, List<Integer>> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, parseArray(p, p::intValue)), field, ValueType.INT_ARRAY);
    }

    private final <T> List<T> parseArray(XContentParser parser, IOSupplier<T> supplier) throws IOException {
        List<T> list = new ArrayList<>();
        if (parser.currentToken().isValue()) {
            list.add(supplier.get()); // single value
        } else {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken().isValue()) {
                    list.add(supplier.get());
                } else {
                    throw new IllegalStateException("expected value but got [" + parser.currentToken() + "]");
                }
            }
        }
        return list;
    }

    public <T> void declareObject(BiConsumer<Value, T> consumer, BiFunction<XContentParser, Context, T> objectParser, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, objectParser.apply(p, c)), field, ValueType.OBJECT);
    }

    public <T> void declareObjectOrDefault(BiConsumer<Value, T> consumer, BiFunction<XContentParser, Context, T> objectParser, Supplier<T> defaultValue, ParseField field) {
        declareField((p, v, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (p.booleanValue()) {
                    consumer.accept(v, defaultValue.get());
                }
            } else {
                consumer.accept(v, objectParser.apply(p, c));
            }
        }, field, ValueType.OBJECT_OR_BOOLEAN);
    }


    public void declareFloat(BiConsumer<Value, Float> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.floatValue()), field, ValueType.FLOAT);
    }

    public void declareDouble(BiConsumer<Value, Double> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.doubleValue()), field, ValueType.DOUBLE);
    }

    public void declareLong(BiConsumer<Value, Long> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.longValue()), field, ValueType.LONG);
    }

    public void declareInt(BiConsumer<Value, Integer> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.intValue()), field, ValueType.INT);
    }

    public void declareValue(BiConsumer<Value, XContentParser> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p), field, ValueType.VALUE);
    }

    public void declareString(BiConsumer<Value, String> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.text()), field, ValueType.STRING);
    }

    public void declareStringOrNull(BiConsumer<Value, String> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text()), field, ValueType.STRING_OR_NULL);
    }

    public void declareBoolean(BiConsumer<Value, Boolean> consumer, ParseField field) {
        declareField((p, v, c) -> consumer.accept(v, p.booleanValue()), field, ValueType.BOOLEAN);
    }

    public static class FieldParser<T> {
        private final Parser parser;
        private final EnumSet<XContentParser.Token> supportedTokens;
        private final ParseField parseField;
        private final ValueType type;

        public FieldParser(Parser parser, EnumSet<XContentParser.Token> supportedTokens, ParseField parseField, ValueType type) {
            this.parser = parser;
            this.supportedTokens = supportedTokens;
            this.parseField = parseField;
            this.type = type;
        }

        public void assertSupports(String parserName, XContentParser.Token token, String currentFieldName, ParseFieldMatcher matcher) {
            if (matcher.match(currentFieldName, parseField) == false) {
                throw new IllegalStateException("[" + parserName  + "] parsefield doesn't accept: " + currentFieldName);
            }
            if (supportedTokens.contains(token) == false) {
                throw new IllegalArgumentException("[" + parserName  + "] " + currentFieldName + " doesn't support values of type: " + token);
            }
        }

        @Override
        public String toString() {
            String[] deprecatedNames = parseField.getDeprecatedNames();
            String allReplacedWith = parseField.getAllReplacedWith();
            return "FieldParser{" +
                    "preferred_name=" + parseField.getPreferredName() +
                    ", supportedTokens=" + supportedTokens +
                    (deprecatedNames == null || deprecatedNames.length == 0 ? "" : ", deprecated_names="  + Arrays.toString(deprecatedNames )) +
                    (allReplacedWith == null ? "" : ", replaced_with=" + allReplacedWith) +
                    ", type=" + type.name() +
                    '}';
        }

    }

    public enum ValueType {
        STRING(EnumSet.of(XContentParser.Token.VALUE_STRING)),
        STRING_OR_NULL(EnumSet.of(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NULL)),
        FLOAT(EnumSet.of(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        DOUBLE(EnumSet.of(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        LONG(EnumSet.of(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        INT(EnumSet.of(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        BOOLEAN(EnumSet.of(XContentParser.Token.VALUE_BOOLEAN)), STRING_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_STRING)),
        FLOAT_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        DOUBLE_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        LONG_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        INT_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_STRING)),
        BOOLEAN_ARRAY(EnumSet.of(XContentParser.Token.START_ARRAY, XContentParser.Token.VALUE_BOOLEAN)),
        OBJECT(EnumSet.of(XContentParser.Token.START_OBJECT)),
        OBJECT_OR_BOOLEAN(EnumSet.of(XContentParser.Token.START_OBJECT, XContentParser.Token.VALUE_BOOLEAN)),
        VALUE(EnumSet.of(XContentParser.Token.VALUE_BOOLEAN, XContentParser.Token.VALUE_NULL ,XContentParser.Token.VALUE_EMBEDDED_OBJECT,XContentParser.Token.VALUE_NUMBER,XContentParser.Token.VALUE_STRING));

        private final EnumSet<XContentParser.Token> tokens;

        ValueType(EnumSet<XContentParser.Token> tokens) {
            this.tokens = tokens;
        }

        public EnumSet<XContentParser.Token> supportedTokens() {
            return this.tokens;
        }
    }

    @Override
    public String toString() {
        return "ObjectParser{" +
                "name='" + name + '\'' +
                ", fields=" + fieldParserMap.values() +
                '}';
    }
}
