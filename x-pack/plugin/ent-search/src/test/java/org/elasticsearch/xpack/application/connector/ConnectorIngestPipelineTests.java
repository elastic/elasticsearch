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

public class ConnectorIngestPipelineTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            ConnectorIngestPipeline testInstance = ConnectorTestUtils.getRandomConnectorIngestPipeline();
            assertTransportSerialization(testInstance);
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "extract_binary_content": true,
                    "name": "search-default-ingestion",
                    "reduce_whitespace": true,
                    "run_ml_inference": false
                }
            """);

        ConnectorIngestPipeline pipeline = ConnectorIngestPipeline.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(pipeline, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorIngestPipeline parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorIngestPipeline.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private void assertTransportSerialization(ConnectorIngestPipeline testInstance) throws IOException {
        ConnectorIngestPipeline deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private ConnectorIngestPipeline copyInstance(ConnectorIngestPipeline instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, ConnectorIngestPipeline::new);
    }

}
