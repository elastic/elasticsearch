/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class InferenceServiceConfigurationTests extends ESTestCase {
    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "service": "some_provider",
               "name": "Some Provider",
               "task_types": ["text_embedding", "completion"],
               "configurations": {
                    "text_field_configuration": {
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important field",
                        "required": true,
                        "sensitive": true,
                        "updatable": false,
                        "type": "str"
                    },
                    "numeric_field_configuration": {
                        "default_value": 3,
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important numeric field",
                        "required": true,
                        "sensitive": false,
                        "updatable": true,
                        "type": "int"
                    }
               }
            }
            """);

        InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
            new BytesArray(content),
            XContentType.JSON
        );
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = InferenceServiceConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_EmptyTaskTypes() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "service": "some_provider",
               "name": "Some Provider",
               "task_types": [],
               "configurations": {
                    "text_field_configuration": {
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important field",
                        "required": true,
                        "sensitive": true,
                        "updatable": false,
                        "type": "str"
                    },
                    "numeric_field_configuration": {
                        "default_value": 3,
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important numeric field",
                        "required": true,
                        "sensitive": false,
                        "updatable": true,
                        "type": "int"
                    }
               }
            }
            """);

        InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
            new BytesArray(content),
            XContentType.JSON
        );
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = InferenceServiceConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToMap() {
        InferenceServiceConfiguration configField = InferenceServiceConfigurationTestUtils.getRandomServiceConfigurationField();
        Map<String, Object> configFieldAsMap = configField.toMap();

        assertThat(configFieldAsMap.get("service"), equalTo(configField.getService()));
        assertThat(configFieldAsMap.get("name"), equalTo(configField.getName()));
        assertThat(configFieldAsMap.get("task_types"), equalTo(configField.getTaskTypes()));
        assertThat(configFieldAsMap.get("configurations"), equalTo(configField.getConfigurations()));
    }
}
