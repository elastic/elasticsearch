/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public interface DocumentParserListener {
    sealed interface Token permits Token.FieldName, Token.StartObject, Token.EndObject, Token.StartArray, Token.EndArray,
        Token.StringAsCharArrayValue, Token.NullValue, Token.ValueToken {

        record FieldName(String name) implements Token {}

        record StartObject() implements Token {}

        record EndObject() implements Token {}

        record StartArray() implements Token {}

        record EndArray() implements Token {}

        record NullValue() implements Token {}

        final class StringAsCharArrayValue implements Token {
            private final XContentParser parser;

            public StringAsCharArrayValue(XContentParser parser) {
                this.parser = parser;
            }

            char[] buffer() throws IOException {
                return parser.textCharacters();
            }

            int length() throws IOException {
                return parser.textLength();
            }

            int offset() throws IOException {
                return parser.textOffset();
            }
        }

        non-sealed interface ValueToken<T> extends Token {
            T value() throws IOException;
        }

        public static Token START_OBJECT = new StartObject();
        public static Token END_OBJECT = new EndObject();
        public static Token START_ARRAY = new StartArray();
        public static Token END_ARRAY = new EndArray();

        static Token current(XContentParser parser) throws IOException {
            return switch (parser.currentToken()) {
                case START_OBJECT -> Token.START_OBJECT;
                case END_OBJECT -> Token.END_OBJECT;
                case START_ARRAY -> Token.START_ARRAY;
                case END_ARRAY -> Token.END_ARRAY;
                case FIELD_NAME -> new FieldName(parser.currentName());
                case VALUE_STRING -> {
                    if (parser.hasTextCharacters()) {
                        yield new StringAsCharArrayValue(parser);
                    } else {
                        yield (ValueToken<String>) parser::text;
                    }
                }
                case VALUE_NUMBER -> switch (parser.numberType()) {
                    case INT -> (ValueToken<Integer>) parser::intValue;
                    case BIG_INTEGER -> (ValueToken<BigInteger>) () -> (BigInteger) parser.numberValue();
                    case LONG -> (ValueToken<Long>) parser::longValue;
                    case FLOAT -> (ValueToken<Float>) parser::floatValue;
                    case DOUBLE -> (ValueToken<Double>) parser::doubleValue;
                    case BIG_DECIMAL -> {
                        // See @XContentGenerator#copyCurrentEvent
                        assert false : "missing xcontent number handling for type [" + parser.numberType() + "]";
                        yield null;
                    }
                };
                case VALUE_BOOLEAN -> (ValueToken<Boolean>) parser::booleanValue;
                case VALUE_EMBEDDED_OBJECT -> (ValueToken<byte[]>) parser::binaryValue;
                case VALUE_NULL -> new NullValue();
                case null -> null;
            };
        }
    }

    sealed interface Event permits Event.DocumentSwitch, Event.DocumentStart, Event.ObjectStart, Event.ObjectEnd, Event.ObjectArrayStart, Event.ObjectArrayEnd {
        record DocumentSwitch(LuceneDocument document) implements Event {}

        record DocumentStart(RootObjectMapper rootObjectMapper) implements Event {}

        record ObjectStart(ObjectMapper objectMapper) implements Event {}

        record ObjectEnd(ObjectMapper objectMapper) implements Event {}

        record ObjectArrayStart(ObjectMapper objectMapper) implements Event {}

        record ObjectArrayEnd() implements Event {}
    }

    record Output(List<IgnoredSourceFieldMapper.NameValue> ignoredSourceValues) {
        static Output empty() {
            return new Output(new ArrayList<>());
        }

        void merge(Output part) {
            this.ignoredSourceValues.addAll(part.ignoredSourceValues);
        }
    }

    /**
     * Specifies if this listener is currently actively consuming tokens.
     * This is used to avoid doing unnecessary work.
     * @return
     */
    boolean isActive();

    void consume(Token token) throws IOException;

    void consume(Event event) throws IOException;

    Output finish();
}
