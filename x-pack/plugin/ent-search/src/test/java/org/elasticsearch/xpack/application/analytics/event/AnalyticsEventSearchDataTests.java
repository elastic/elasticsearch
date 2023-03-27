/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_QUERY_FIELD;

public class AnalyticsEventSearchDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSearchData> {

    public static AnalyticsEventSearchData randomEventSearchData() {
        return new AnalyticsEventSearchData(randomIdentifier());
    }

    public void testToXContent() throws IOException {
        AnalyticsEventSearchData search = randomEventSearchData();

        // Serialize the search data
        BytesReference json = XContentHelper.toXContent(search, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(1, contentAsMap.size());
        assertTrue(contentAsMap.containsKey(SEARCH_QUERY_FIELD.getPreferredName()));
        assertEquals(search.query(), contentAsMap.get(SEARCH_QUERY_FIELD.getPreferredName()));

        // Check we can re-serialize with fromXContent and object are equals
        assertEquals(search, parseSearchData(json));
    }

    public void testFromXContent() throws IOException {
        String query = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SEARCH_QUERY_FIELD.getPreferredName(), query).map();
        AnalyticsEventSearchData search = parseSearchData(convertMapToJson(jsonMap));
        assertEquals(query, search.query());
    }

    public void testFromXContentWhenQueryFieldIsMissing() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}}]", SEARCH_QUERY_FIELD),
            () -> parseSearchData(new BytesArray("{}"))
        );
    }

    public void testFromXContentWithAnInvalidFields() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SEARCH_QUERY_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SEARCH_FIELD, invalidFieldName),
            () -> parseSearchData(convertMapToJson(jsonMap))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSearchData> instanceReader() {
        return AnalyticsEventSearchData::new;
    }

    @Override
    protected AnalyticsEventSearchData createTestInstance() {
        return randomEventSearchData();
    }

    @Override
    protected AnalyticsEventSearchData mutateInstance(AnalyticsEventSearchData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventSearchData parseSearchData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSearchData.fromXContent(contentParser, null);
        }
    }

    private static BytesReference convertMapToJson(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder);
        }
    }
}
