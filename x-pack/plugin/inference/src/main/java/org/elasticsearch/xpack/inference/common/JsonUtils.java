/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

public class JsonUtils {

    /**
     * Converts an object into a JSON value.
     * @param value the generic object to serialize, this must be a type that the {@link JsonXContent} provider supports.
     * @param field a description of the object being serialized, can be the name of the field
     * @return a String representation of the object serialized
     * @param <T> the type of the object being serialized
     */
    public static <T> String toJson(T value, String field) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.value(value);
            return Strings.toString(builder);
        } catch (Exception e) {
            throw new IllegalStateException(
                Strings.format("Failed to serialize value as JSON, field: %s, error: %s", field, e.getMessage()),
                e
            );
        }
    }

    private JsonUtils() {}
}
