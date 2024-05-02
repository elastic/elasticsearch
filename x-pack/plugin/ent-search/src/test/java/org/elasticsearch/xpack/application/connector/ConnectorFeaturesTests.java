/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class ConnectorFeaturesTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            ConnectorFeatures testInstance = ConnectorTestUtils.getRandomConnectorFeatures();
            assertTransportSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "document_level_security": {
                        "enabled": true
                    },
                    "filtering_advanced_config": true,
                    "sync_rules": {
                        "advanced": {
                            "enabled": false
                        },
                        "basic": {
                            "enabled": true
                        }
                    }
                }
            """);

        testToXContentChecker(content);
    }

    public void testToXContentMissingDocumentLevelSecurity() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "filtering_advanced_config": true,
                    "sync_rules": {
                        "advanced": {
                            "enabled": false
                        },
                        "basic": {
                            "enabled": true
                        }
                    }
                }
            """);

        testToXContentChecker(content);
    }

    public void testToXContentMissingSyncRules() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "filtering_advanced_config": true
                }
            """);

        testToXContentChecker(content);
    }

    public void testToXContentMissingSyncRulesAdvanced() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "filtering_advanced_config": true,
                    "sync_rules": {
                        "basic": {
                            "enabled": true
                        }
                    }
                }
            """);

        testToXContentChecker(content);
    }

    public void testToXContent_NativeConnectorAPIKeysEnabled() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "document_level_security": {
                        "enabled": true
                    },
                    "filtering_advanced_config": true,
                    "sync_rules": {
                        "advanced": {
                            "enabled": false
                        },
                        "basic": {
                            "enabled": true
                        }
                    },
                    "native_connector_api_keys": {
                        "enabled": true
                    }
                }
            """);

        testToXContentChecker(content);
    }

    private void testToXContentChecker(String content) throws IOException {
        ConnectorFeatures features = ConnectorFeatures.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(features, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorFeatures parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorFeatures.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(ConnectorFeatures testInstance) throws IOException {
        ConnectorFeatures deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private ConnectorFeatures copyInstance(ConnectorFeatures instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, ConnectorFeatures::new);
    }
}
