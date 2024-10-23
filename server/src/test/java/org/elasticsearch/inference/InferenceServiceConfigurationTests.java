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
               "provider": "some_provider",
               "task_types": [
                  "text_embedding",
                  "completion"
               ],
               "configuration": {
                    "text_field_configuration": {
                        "default_value": null,
                        "depends_on": [
                            {
                                "field": "some_field",
                                "value": true
                            }
                        ],
                        "display": "textbox",
                        "label": "Very important field",
                        "options": [],
                        "order": 4,
                        "required": true,
                        "sensitive": true,
                        "tooltip": "Wow, this tooltip is useful.",
                        "type": "str",
                        "ui_restrictions": [],
                        "validations": null,
                        "value": ""
                    },
                    "numeric_field_configuration": {
                        "default_value": 3,
                        "depends_on": null,
                        "display": "numeric",
                        "label": "Very important numeric field",
                        "options": [],
                        "order": 2,
                        "required": true,
                        "sensitive": false,
                        "tooltip": "Wow, this tooltip is useful.",
                        "type": "int",
                        "ui_restrictions": [],
                        "validations": [
                            {
                                "constraint": 0,
                                "type": "greater_than"
                            }
                        ],
                        "value": ""
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

        assertThat(configFieldAsMap.get("provider"), equalTo(configField.getProvider()));
        assertThat(configFieldAsMap.get("task_types"), equalTo(configField.getTaskTypes()));
        assertThat(configFieldAsMap.get("configuration"), equalTo(configField.getConfiguration()));
    }
}
