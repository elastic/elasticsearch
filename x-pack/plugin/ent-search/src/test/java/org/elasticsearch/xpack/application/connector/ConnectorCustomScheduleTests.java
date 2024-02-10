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

public class ConnectorCustomScheduleTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            ConnectorCustomSchedule testInstance = ConnectorTestUtils.getRandomConnectorCustomSchedule();
            assertTransportSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "configuration_overrides": {
                        "domain_allowlist": [
                            "https://example.com"
                        ],
                        "max_crawl_depth": 1,
                        "seed_urls": [
                            "https://example.com/blog",
                            "https://example.com/info"
                        ],
                        "sitemap_discovery_disabled": true,
                        "sitemap_urls": [
                            "https://example.com/sitemap.xml"
                        ]
                    },
                    "enabled": true,
                    "interval": "0 0 12 * * ?",
                    "last_synced": null,
                    "name": "My Schedule"
                }
            """);

        ConnectorCustomSchedule customSchedule = ConnectorCustomSchedule.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(customSchedule, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorCustomSchedule parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorCustomSchedule.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(ConnectorCustomSchedule testInstance) throws IOException {
        ConnectorCustomSchedule deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private ConnectorCustomSchedule copyInstance(ConnectorCustomSchedule instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, ConnectorCustomSchedule::new);
    }
}
