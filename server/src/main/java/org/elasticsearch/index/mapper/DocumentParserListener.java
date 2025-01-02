/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Component that listens to events produced by {@link DocumentParser} in order to implement some parsing related logic.
 * It allows to keep such logic separate from actual document parsing workflow which is by itself complex.
 */
public interface DocumentParserListener {
    /**
     * Specifies if this listener is currently actively consuming tokens.
     * This is used to avoid doing unnecessary work.
     * @return
     */
    boolean isActive();

    /**
     * Sends a {@link Token} to this listener.
     * This is only called when {@link #isActive()} returns true since it involves a somewhat costly operation of creating a token instance
     * and tokens are low level meaning this is called very frequently.
     * @param token
     * @throws IOException
     */
    void consume(Token token) throws IOException;

    /**
     * Sends an {@link Event} to this listener. Unlike tokens events are always sent to a listener.
     * The logic here is that based on the event listener can decide to change the return value of {@link #isActive()}.
     * @param event
     * @throws IOException
     */
    void consume(Event event) throws IOException;

    Output finish();

    /**
     * A lower level notification passed from the parser to a listener.
     * This token is closely related to {@link org.elasticsearch.xcontent.XContentParser.Token} and is used for use cases like
     * preserving the exact structure of the parsed document.
     */
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

        Token START_OBJECT = new StartObject();
        Token END_OBJECT = new EndObject();
        Token START_ARRAY = new StartArray();
        Token END_ARRAY = new EndArray();

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

    /**
     * High level notification passed from the parser to a listener.
     * Events represent meaningful logical operations during parsing and contain relevant context for the operation
     * like a mapper being used.
     * A listener can use events and/or tokens depending on the use case. For example, it can wait for a specific event and then switch
     * to consuming tokens instead.
     */
    sealed interface Event permits Event.DocumentStart, Event.ObjectStart, Event.ObjectArrayStart, Event.LeafArrayStart, Event.LeafValue {
        record DocumentStart(RootObjectMapper rootObjectMapper, LuceneDocument document) implements Event {}

        record ObjectStart(ObjectMapper objectMapper, boolean insideObjectArray, ObjectMapper parentMapper, LuceneDocument document)
            implements
                Event {}

        record ObjectArrayStart(ObjectMapper objectMapper, ObjectMapper parentMapper, LuceneDocument document) implements Event {}

        final class LeafValue implements Event {
            private final FieldMapper fieldMapper;
            private final boolean insideObjectArray;
            private final ObjectMapper parentMapper;
            private final LuceneDocument document;
            private final XContentParser parser;
            private final boolean isObjectOrArray;
            private final boolean isArray;

            public LeafValue(
                FieldMapper fieldMapper,
                boolean insideObjectArray,
                ObjectMapper parentMapper,
                LuceneDocument document,
                XContentParser parser
            ) {
                this.fieldMapper = fieldMapper;
                this.insideObjectArray = insideObjectArray;
                this.parentMapper = parentMapper;
                this.document = document;
                this.parser = parser;
                this.isObjectOrArray = parser.currentToken().isValue() == false && parser.currentToken() != XContentParser.Token.VALUE_NULL;
                this.isArray = parser.currentToken() == XContentParser.Token.START_ARRAY;
            }

            public FieldMapper fieldMapper() {
                return fieldMapper;
            }

            public boolean insideObjectArray() {
                return insideObjectArray;
            }

            public ObjectMapper parentMapper() {
                return parentMapper;
            }

            public LuceneDocument document() {
                return document;
            }

            /**
             * @return whether a value is an object or an array vs a single value like a long.
             */
            boolean isContainer() {
                return isObjectOrArray;
            }

            boolean isArray() {
                return isArray;
            }

            BytesRef encodeValue() throws IOException {
                assert isContainer() == false : "Objects should not be handled with direct encoding";

                return XContentDataHelper.encodeToken(parser);
            }
        }

        record LeafArrayStart(FieldMapper fieldMapper, ObjectMapper parentMapper, LuceneDocument document) implements Event {}
    }

    record Output(List<IgnoredSourceFieldMapper.NameValue> ignoredSourceValues) {
        static Output empty() {
            return new Output(new ArrayList<>());
        }

        void merge(Output part) {
            this.ignoredSourceValues.addAll(part.ignoredSourceValues);
        }
    }
}
