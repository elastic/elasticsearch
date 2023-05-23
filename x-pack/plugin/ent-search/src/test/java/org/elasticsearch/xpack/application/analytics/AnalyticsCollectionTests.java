/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.TransportVersion;
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
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;

public class AnalyticsCollectionTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    public void testDataStreamName() {
        AnalyticsCollection collection = randomAnalyticsCollection();
        String expectedDataStreamName = EVENT_DATA_STREAM_INDEX_PREFIX + collection.getName();
        assertEquals(expectedDataStreamName, collection.getEventDataStream());
    }

    public final void testRandomSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            AnalyticsCollection collection = randomAnalyticsCollection();
            assertTransportSerialization(collection, TransportVersion.CURRENT);
            assertXContent(collection, randomBoolean());
        }
    }

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
            { }
            """);
        AnalyticsCollection collection = AnalyticsCollection.fromXContentBytes("my_collection", new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(collection, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        AnalyticsCollection parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = AnalyticsCollection.fromXContent(collection.getName(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    private AnalyticsCollection assertXContent(AnalyticsCollection collection, boolean humanReadable) throws IOException {
        BytesReference originalBytes = toShuffledXContent(collection, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        AnalyticsCollection parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = AnalyticsCollection.fromXContent(collection.getName(), parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
        return parsed;
    }

    private AnalyticsCollection assertTransportSerialization(AnalyticsCollection testInstance, TransportVersion version)
        throws IOException {
        AnalyticsCollection deserializedInstance = copyInstance(testInstance, version);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
        return deserializedInstance;
    }

    private AnalyticsCollection copyInstance(AnalyticsCollection instance, TransportVersion version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, AnalyticsCollection::new, version);
    }

    private static AnalyticsCollection randomAnalyticsCollection() {
        return new AnalyticsCollection(randomIdentifier());
    }
}
