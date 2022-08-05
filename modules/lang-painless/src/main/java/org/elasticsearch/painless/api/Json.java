/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class Json {
    /**
     * Load a string as the Java version of a JSON type, either List (JSON array), Map (JSON object), Number, Boolean or String
     */
    public static Object load(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json);

        return switch (parser.nextToken()) {
            case START_ARRAY -> parser.list();
            case START_OBJECT -> parser.map();
            case VALUE_NUMBER -> parser.numberValue();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case VALUE_STRING -> parser.text();
            default -> null;
        };
    }

    /**
     * Write a JSON representable type as a string
     */
    public static String dump(Object data) throws IOException {
        return dump(data, false);
    }

    /**
     * Write a JSON representable type as a string, optionally pretty print it by spanning multiple lines and indenting
     */
    public static String dump(Object data, boolean pretty) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        if (pretty) {
            builder.prettyPrint();
        }
        builder.value(data);
        builder.flush();
        return builder.getOutputStream().toString();
    }
}
