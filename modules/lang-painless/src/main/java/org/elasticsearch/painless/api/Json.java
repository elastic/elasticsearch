/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;

public class Json {
    /**
     * Load a string as the Java version of a JSON type, either List (JSON array), Map (JSON object), Number, Boolean or String
     */
    public static Object load(String json) throws IOException{
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json);

        switch (parser.nextToken()) {
            case START_ARRAY:
                return parser.list();
            case START_OBJECT:
                return parser.map();
            case VALUE_NUMBER:
                return parser.numberValue();
            case VALUE_BOOLEAN:
                return parser.booleanValue();
            case VALUE_STRING:
                return parser.text();
            default:
                return null;
        }
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
