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
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(field)) {
                parser.nextToken();
                return;
            }
        }

        throw new IllegalStateException(format(errorMsgTemplate, field));
    }

    private XContentUtils() {}
}
