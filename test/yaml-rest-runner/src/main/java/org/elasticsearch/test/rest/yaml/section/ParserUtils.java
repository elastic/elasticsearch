/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 * Utility methods used for parsing test sections.
 */
class ParserUtils {
    private ParserUtils() {
        // Do not build.
    }

    public static String parseField(XContentParser parser) throws IOException {
        parser.nextToken();
        assert parser.currentToken().isValue();
        String field = parser.text();
        parser.nextToken();
        return field;
    }

    public static Tuple<String, Object> parseTuple(XContentParser parser) throws IOException {
        parser.nextToken();
        advanceToFieldName(parser);
        Map<String, Object> map = parser.map();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        parser.nextToken();

        if (map.size() != 1) {
            throw new IllegalArgumentException("expected key value pair but found an object with " + map.size() + " fields");
        }

        Map.Entry<String, Object> entry = map.entrySet().iterator().next();
        return Tuple.tuple(entry.getKey(), entry.getValue());
    }

    public static void advanceToFieldName(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        // we are in the beginning, haven't called nextToken yet
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException(
                "malformed test section: field name expected but found " + token + " at " + parser.getTokenLocation()
            );
        }
    }
}
