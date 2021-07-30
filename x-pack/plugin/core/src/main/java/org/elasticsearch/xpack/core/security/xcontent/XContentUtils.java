/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XContentUtils {

    private XContentUtils() {
    }

    /**
     * Ensures that we're currently on the start of an object, or that the next token is a start of an object.
     *
     * @throws ElasticsearchParseException if the current or the next token is a {@code START_OBJECT}
     */
    public static void verifyObject(XContentParser parser) throws IOException, ElasticsearchParseException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return;
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected an object, but found token [{}]", parser.currentToken());
        }
    }

    public static String[] readStringArray(XContentParser parser, boolean allowNull) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            if (allowNull) {
                return null;
            }
            throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but found null value instead",
                    parser.currentName());
        }
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but found [{}] value instead",
                    parser.currentName(), parser.currentToken());
        }

        List<String> list = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                list.add(parser.text());
            } else {
                throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but one of the value in the " +
                        "array is [{}]", parser.currentName(), token);
            }
        }
        return list.toArray(new String[list.size()]);
    }
}
