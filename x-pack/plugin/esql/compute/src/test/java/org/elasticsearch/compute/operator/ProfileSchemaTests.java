/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ProfileSchemaTests extends ESTestCase {

    public void testWritesObjectAndDescribesItsFields() throws IOException {
        ProfileSchema<Value> schema = ProfileSchema.<Value>object((builder, value, params) -> {
            builder.startObject();
            builder.field("name", value.name());
            if (value.count() > 0) {
                builder.field("count", value.count());
            }
            return builder.endObject();
        }).required("name", ProfileSchema.string()).optional("count", ProfileSchema.integer()).build();

        var builder = XContentFactory.jsonBuilder();
        schema.write(builder, new Value("test", 1), ToXContent.EMPTY_PARAMS);
        assertEquals("{\"name\":\"test\",\"count\":1}", Strings.toString(builder));
        assertEquals("object", schema.schema().get("type"));
        assertEquals(List.of("name"), schema.schema().get("required"));
        assertEquals(false, schema.schema().get("additionalProperties"));
        assertEquals(Map.of("name", ProfileSchema.string(), "count", ProfileSchema.integer()), schema.schema().get("properties"));
    }

    public void testRejectsDuplicateProperty() {
        ProfileSchema.Builder<Value> builder = ProfileSchema.object((xContent, value, params) -> xContent);
        builder.required("name", ProfileSchema.string());
        expectThrows(IllegalArgumentException.class, () -> builder.optional("name", ProfileSchema.string()));
    }

    private record Value(String name, long count) {}
}
