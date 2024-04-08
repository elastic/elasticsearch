/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

public class XContentUtils {

    /**
     * Moves to the first valid token, which is non-null.
     * Does not move, if the parser is already positioned at a valid token.
     *
     * @param parser parser to move
     * @throws IOException if underlying parser methods throw
     */
    public static void moveToFirstToken(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
    }

    /**
     * Iterates over the tokens until it finds a field name token with the text matching the field requested.
     *
     * @param parser parser to move
     * @param field the field name to find
     * @param errorMsgTemplate a template message to populate an exception if the field cannot be found
     * @throws IllegalStateException if the field cannot be found
     */
    public static void positionParserAtTokenAfterField(XContentParser parser, String field, String errorMsgTemplate) throws IOException {
        XContentParser.Token token = parser.nextToken();

        while (token != null && token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(field)) {
                parser.nextToken();
                return;
            }
            token = parser.nextToken();
        }

        throw new IllegalStateException(format(errorMsgTemplate, field));
    }

    /**
     * Progress the parser consuming and discarding tokens until the
     * parser points to the end of the current object. Nested objects
     * and arrays are skipped.
     *
     * If successful the parser's current token is the end object token.
     *
     * @param parser
     * @throws IOException
     */
    public static void consumeUntilObjectEnd(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();

        // token == null when correctly formed input has
        // been fully parsed.
        while (token != null && token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }

            token = parser.nextToken();
        }
    }

    private XContentUtils() {}
}
