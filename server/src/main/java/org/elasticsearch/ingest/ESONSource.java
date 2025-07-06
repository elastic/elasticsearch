/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ESONSource {

    public static class Builder {

        private final BytesStreamOutput bytes = new BytesStreamOutput();

        public Object parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            return parseObject(parser);
        }

        private Object parseObject(XContentParser parser) throws IOException {
            HashMap<String, Type> map = new HashMap<>();
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.FIELD_NAME || token == XContentParser.Token.END_OBJECT;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case START_OBJECT:
                        parseObject(parser);
                        break;
                    case START_ARRAY:
                        parseArray(parser);
                        break;
                    case VALUE_NULL:
                        map.put(currentFieldName, new NullValue());
                        break;
                    default:
                        if (token.isValue()) {
                            map.put(currentFieldName, parseValue(parser, token));
                        }
                }
            }
            return new Object(map);
        }

        private Array parseArray(XContentParser parser) {
            return new Array();
        }

        private Value parseValue(XContentParser parser, XContentParser.Token token) throws IOException {
            long position = bytes.position();
            switch (token) {
                case VALUE_NUMBER -> handleNumber(parser);
                case VALUE_STRING -> {
                    // Or null variant?
                    XContentString xContentString = parser.optimizedText();
                    xContentString.bytes();
                    System.err.println(xContentString.string());
                }
                case VALUE_BOOLEAN -> System.err.println(parser.booleanValue());
                case VALUE_EMBEDDED_OBJECT -> System.err.println(parser.binaryValue());
                default -> throw new IllegalStateException("Unexpected token [" + token + "]");
            }
            return new Value(Math.toIntExact(position));
        }

        private static void handleNumber(XContentParser parser) throws IOException {
            switch (parser.numberType()) {


            }
        }

    }

    interface Type {}

    record Object(Map<String, Type> map) implements Type {}

    record Array() implements Type {}

    record Value(int position) implements Type {}

    record NullValue() implements Type {}
}
