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

public interface DocumentParserListener {
    sealed interface Token permits Token.FieldName, Token.StartObject, Token.EndObject, Token.StartArray, Token.EndArray, Token.StringValue,
        Token.BooleanValue, Token.IntValue {
        record FieldName(String name) implements Token {}

        record StartObject() implements Token {}

        record EndObject() implements Token {}

        record StartArray() implements Token {}

        record EndArray() implements Token {}

        record StringValue(String value) implements Token {}

        record BooleanValue(boolean value) implements Token {}

        record IntValue(int value) implements Token {}

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
                    case BIG_INTEGER -> null;
                    case LONG -> null;
                    case FLOAT -> null;
                    case DOUBLE -> null;
                    case BIG_DECIMAL -> null;
                };
                case VALUE_BOOLEAN -> new BooleanValue(parser.booleanValue());
                case VALUE_EMBEDDED_OBJECT -> null;
                case VALUE_NULL -> null;
                case null -> null;
            };
        }
    }

    void consume(Token token) throws IOException;
}
