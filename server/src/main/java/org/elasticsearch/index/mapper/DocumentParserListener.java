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

public interface DocumentParserListener {
    sealed interface Token permits Token.FieldName, Token.StartObject, Token.EndObject, Token.StartArray, Token.EndArray,
        Token.StringValue {
        record FieldName(String name) implements Token {}

        record StartObject() implements Token {}

        record EndObject() implements Token {}

        record StartArray() implements Token {}

        record EndArray() implements Token {}

        record StringValue() implements Token {}

        static Token current(XContentParser parser) {
            return new Token.StartArray();
        }
    }

    void consume(Token token);
}
