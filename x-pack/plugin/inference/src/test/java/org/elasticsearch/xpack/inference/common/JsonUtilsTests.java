/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;
import static org.hamcrest.Matchers.is;

public class JsonUtilsTests extends ESTestCase {
    public void testToJson() throws IOException {
        assertThat(toJson("string", "field"), is("\"string\""));
        assertThat(toJson(1, "field"), is("1"));
        assertThat(toJson(List.of("a", "b"), "field"), is("[\"a\",\"b\"]"));

        {
            var expected = XContentHelper.stripWhitespace("""
                {
                  "key": "value",
                  "key2": [1, 2]
                }
                """);
            assertThat(toJson(new TreeMap<>(Map.of("key", "value", "key2", List.of(1, 2))), "field"), is(expected));
        }
        {
            var serializer = new ToXContentFragment() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.value("string");
                    return builder;
                }
            };

            assertThat(toJson(serializer, "field"), is("\"string\""));
        }
        assertThat(toJson(1.1d, "field"), is("1.1"));
        assertThat(toJson(1.1f, "field"), is("1.1"));
        assertThat(toJson(true, "field"), is("true"));
        assertThat(toJson(false, "field"), is("false"));
        assertThat(toJson(new SerializableSecureString("api_key"), "field"), is("\"api_key\""));
    }

    public void testToJson_ThrowsException_WhenUnableToSerialize() {
        var exception = expectThrows(IllegalStateException.class, () -> toJson(new SecureString("string".toCharArray()), "field"));
        assertThat(
            exception.getMessage(),
            is(
                "Failed to serialize value as JSON, field: field, error: "
                    + "cannot write xcontent for unknown value of type class org.elasticsearch.common.settings.SecureString"
            )
        );
    }

    public void testToJson_ThrowsException_WhenValueSerializationFails() {
        var failingToXContent = new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IOException("failed");
            }
        };

        var exception = expectThrows(IllegalStateException.class, () -> toJson(failingToXContent, "field"));
        assertThat(exception.getMessage(), is("Failed to serialize value as JSON, field: field, error: failed"));
    }
}
