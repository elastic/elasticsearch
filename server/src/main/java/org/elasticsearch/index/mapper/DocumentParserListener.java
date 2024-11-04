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

public interface DocumentParserListener {
    sealed interface Token permits Token.FieldName, Token.StartObject, Token.EndObject, Token.StartArray, Token.EndArray, Token.StringValue,
        Token.BooleanValue, Token.IntValue, Token.BigIntegerValue, Token.LongValue, Token.DoubleValue, Token.FloatValue, Token.NullValue {
        record FieldName(String name) implements Token {}

        record StartObject() implements Token {}

        record EndObject() implements Token {}

        record StartArray() implements Token {}

        record EndArray() implements Token {}

        record StringValue(String value) implements Token {}

        record BooleanValue(boolean value) implements Token {}

        record IntValue(int value) implements Token {}

        record LongValue(long value) implements Token {}

        record BigIntegerValue(BigInteger value) implements Token {}

        record DoubleValue(double value) implements Token {}

        record FloatValue(float value) implements Token {}

        record NullValue() implements Token {}

        static Token current(XContentParser parser) throws IOException {
            return switch (parser.currentToken()) {
                case START_OBJECT -> new StartObject();
                case END_OBJECT -> new EndObject();
                case START_ARRAY -> new StartArray();
                case END_ARRAY -> new EndArray();
                case FIELD_NAME -> new FieldName(parser.currentName());
                case VALUE_STRING -> new StringValue(parser.text());
                case VALUE_NUMBER -> switch (parser.numberType()) {
                    case INT -> new Token.IntValue(parser.intValue());
                    case BIG_INTEGER -> new BigIntegerValue((BigInteger) parser.numberValue());
                    case LONG -> new Token.LongValue(parser.longValue());
                    case FLOAT -> new Token.FloatValue(parser.floatValue());
                    case DOUBLE -> new Token.DoubleValue(parser.doubleValue());
                    case BIG_DECIMAL -> {
                        // See @XContentGenerator#copyCurrentEvent
                        assert false : "missing xcontent number handling for type [" + parser.numberType() + "]";
                        yield null;
                    }
                };
                case VALUE_BOOLEAN -> new BooleanValue(parser.booleanValue());
                // TODO
                case VALUE_EMBEDDED_OBJECT -> null;
                case VALUE_NULL -> new NullValue();
                case null -> null;
            };
        }
    }

    sealed interface Event permits Event.DocumentSwitch {
        record DocumentSwitch(LuceneDocument document) implements Event {}
    }

    void consume(Token token) throws IOException;

    void consume(Event event);
}
