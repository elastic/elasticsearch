/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
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
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            BytesReference json = BytesReference.bytes(search.toXContent(builder, ToXContent.EMPTY_PARAMS));

            // Check the content that have been processed.
            Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
            assertEquals(1, contentAsMap.size());
            assertTrue(contentAsMap.containsKey(SEARCH_QUERY_FIELD.getPreferredName()));
            assertEquals(search.query(), contentAsMap.get(SEARCH_QUERY_FIELD.getPreferredName()));

            // Check we can reserialize with fromXContent and object are equals
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
                assertEquals(search, AnalyticsEventSearchData.fromXContent(parser, null));
            }
        }
    }

    public void testFromXContent() throws IOException {
        String query = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SEARCH_QUERY_FIELD.getPreferredName(), query).map();
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap)) {
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder));
            AnalyticsEventSearchData search = AnalyticsEventSearchData.fromXContent(parser, null);
            assertEquals(query, search.query());
        }
    }

    public void testFromXContentWhenQueryFieldIsMissing() throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, "{}")) {
            expectThrows(
                IllegalArgumentException.class,
                LoggerMessageFormat.format("Required [{}}]", SEARCH_QUERY_FIELD),
                () -> AnalyticsEventSearchData.fromXContent(parser, null)
            );
        }
    }

    public void testFromXContentWithAnInvalidFields() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SEARCH_QUERY_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SEARCH_FIELD, invalidFieldName),
                () -> AnalyticsEventSearchData.fromXContent(parser, null)
            );
        }
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
}
