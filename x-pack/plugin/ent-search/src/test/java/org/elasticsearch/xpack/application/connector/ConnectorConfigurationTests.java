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

public class ConnectorConfigurationTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            ConnectorConfiguration testInstance = ConnectorTestUtils.getRandomConnectorConfigurationField();
            assertTransportSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
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
               "sensitive": false,
               "tooltip": "Wow, this tooltip is useful.",
               "type": "str",
               "ui_restrictions": [],
               "validations": [
                  {
                     "constraint": 0,
                     "type": "greater_than"
                  }
               ],
               "value": ""
            }
            """);

        ConnectorConfiguration configuration = ConnectorConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentCrawlerConfig_WithNullValue() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "label": "nextSyncConfig",
               "value": null
            }
            """);

        ConnectorConfiguration configuration = ConnectorConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentCrawlerConfig_WithCrawlerConfigurationOverrides() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
               "label": "nextSyncConfig",
               "value": {
                   "max_crawl_depth": 3,
                   "sitemap_discovery_disabled": false,
                   "seed_urls": ["https://elastic.co/"]
               }
            }
            """);

        ConnectorConfiguration configuration = ConnectorConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContentWithMultipleConstraintTypes() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            {
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
               "sensitive": false,
               "tooltip": "Wow, this tooltip is useful.",
               "type": "str",
               "ui_restrictions": [],
               "validations": [
                   {
                       "constraint": 32,
                       "type": "less_than"
                   },
                   {
                       "constraint": "^\\\\\\\\d{4}-\\\\\\\\d{2}-\\\\\\\\d{2}$",
                       "type": "regex"
                   },
                   {
                       "constraint": "int",
                       "type": "list_type"
                   },
                   {
                       "constraint": [
                           1,
                           2,
                           3
                       ],
                       "type": "included_in"
                   },
                   {
                       "constraint": [
                           "string_1",
                           "string_2",
                           "string_3"
                       ],
                       "type": "included_in"
                   }
               ],
               "value": ""
            }
            """);

        ConnectorConfiguration configuration = ConnectorConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorConfiguration parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(ConnectorConfiguration testInstance) throws IOException {
        ConnectorConfiguration deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private ConnectorConfiguration copyInstance(ConnectorConfiguration instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, ConnectorConfiguration::new);
    }
}
