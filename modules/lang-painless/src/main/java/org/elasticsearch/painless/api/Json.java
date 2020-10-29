/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
