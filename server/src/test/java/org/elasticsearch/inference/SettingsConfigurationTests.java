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

public class SettingsConfigurationTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "description": "Wow, this tooltip is useful.",
               "label": "Very important field",
               "required": true,
               "sensitive": false,
               "updatable": true,
               "type": "str",
               "supported_task_types": ["text_embedding", "completion", "sparse_embedding", "rerank"]
            }
            """);

        SettingsConfiguration configuration = SettingsConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        SettingsConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = SettingsConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_WithNumericSelectOptions() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "description": "Wow, this tooltip is useful.",
               "label": "Very important field",
               "required": true,
               "sensitive": false,
               "updatable": true,
               "type": "str",
               "supported_task_types": ["text_embedding"]
            }
            """);

        SettingsConfiguration configuration = SettingsConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        SettingsConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = SettingsConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentCrawlerConfig_WithNullValue() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "label": "nextSyncConfig",
               "value": null,
               "supported_task_types": ["text_embedding", "completion", "sparse_embedding", "rerank"]
            }
            """);

        SettingsConfiguration configuration = SettingsConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        SettingsConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = SettingsConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToMap() {
        SettingsConfiguration configField = SettingsConfigurationTestUtils.getRandomSettingsConfigurationField();
        Map<String, Object> configFieldAsMap = configField.toMap();

        assertThat(configFieldAsMap.get("default_value"), equalTo(configField.getDefaultValue()));

        if (configField.getDescription() != null) {
            assertThat(configFieldAsMap.get("description"), equalTo(configField.getDescription()));
        } else {
            assertFalse(configFieldAsMap.containsKey("description"));
        }

        assertThat(configFieldAsMap.get("label"), equalTo(configField.getLabel()));

        assertThat(configFieldAsMap.get("required"), equalTo(configField.isRequired()));
        assertThat(configFieldAsMap.get("sensitive"), equalTo(configField.isSensitive()));
        assertThat(configFieldAsMap.get("updatable"), equalTo(configField.isUpdatable()));

        if (configField.getType() != null) {
            assertThat(configFieldAsMap.get("type"), equalTo(configField.getType().toString()));
        } else {
            assertFalse(configFieldAsMap.containsKey("type"));
        }
    }
}
