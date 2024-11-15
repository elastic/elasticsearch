/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class InferenceServiceConfigurationTests extends AbstractWireSerializingTestCase<InferenceServiceConfiguration> {
    /**
     * The bwc versions to test serialization against
     */
    protected List<TransportVersion> bwcVersions() {
        return DEFAULT_BWC_VERSIONS;
    }

    /**
     * Test serialization and deserialization of the test instance across versions
     */
    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            InferenceServiceConfiguration testInstance = createTestInstance();
            for (TransportVersion bwcVersion : bwcVersions()) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    /**
     * Assert that instances copied at a particular version are equal. The version is useful
     * for sanity checking the backwards compatibility of the wire. It isn't a substitute for
     * real backwards compatibility tests but it is *so* much faster.
     */
    protected final void assertBwcSerialization(InferenceServiceConfiguration testInstance, TransportVersion version) throws IOException {
        InferenceServiceConfiguration deserializedInstance = copyWriteable(
            testInstance,
            getNamedWriteableRegistry(),
            instanceReader(),
            version
        );
        assertOnBWCObject(deserializedInstance, mutateInstanceForVersion(testInstance, version), version);
    }

    /**
     * @param bwcSerializedObject The object deserialized from the previous version
     * @param testInstance The original test instance
     * @param version The version which serialized
     */
    protected void assertOnBWCObject(
        InferenceServiceConfiguration bwcSerializedObject,
        InferenceServiceConfiguration testInstance,
        TransportVersion version
    ) {
        var errorMessage = Strings.format("Failed for TransportVersion [%s]", version.toString());

        assertNotSame(errorMessage, bwcSerializedObject, testInstance);
        assertEquals(errorMessage, bwcSerializedObject, testInstance);
        assertEquals(errorMessage, bwcSerializedObject.hashCode(), testInstance.hashCode());
    }

    @Override
    protected Writeable.Reader<InferenceServiceConfiguration> instanceReader() {
        return InferenceServiceConfiguration::new;
    }

    @Override
    protected InferenceServiceConfiguration createTestInstance() {
        return InferenceServiceConfigurationTestUtils.getRandomServiceConfigurationField();
    }

    @Override
    protected InferenceServiceConfiguration mutateInstance(InferenceServiceConfiguration instance) throws IOException {
        return randomValueOtherThan(instance, InferenceServiceConfigurationTestUtils::getRandomServiceConfigurationField);
    }

    protected InferenceServiceConfiguration mutateInstanceForVersion(InferenceServiceConfiguration instance, TransportVersion version) {
        if (version.before(TransportVersions.INFERENCE_SERVICES_NAME_ICON_ADDED)) {
            // default to null name and icon if node is on a version before name and icon were added
            return new InferenceServiceConfiguration(
                instance.getProvider(),
                null,
                null,
                instance.getTaskTypes(),
                instance.getConfiguration()
            );
        }
        return instance;
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "provider": "some_provider",
               "name": "Some Provider",
               "icon": "someProviderIcon",
               "task_types": [
                  {
               "task_type": "text_embedding",
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
            },
            {
               "task_type": "completion",
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
        assertThat(configFieldAsMap.get("name"), equalTo(configField.getName()));
        assertThat(configFieldAsMap.get("icon"), equalTo(configField.getIcon()));
        assertThat(configFieldAsMap.get("task_types"), equalTo(configField.getTaskTypes()));
        assertThat(configFieldAsMap.get("configuration"), equalTo(configField.getConfiguration()));
    }
}
