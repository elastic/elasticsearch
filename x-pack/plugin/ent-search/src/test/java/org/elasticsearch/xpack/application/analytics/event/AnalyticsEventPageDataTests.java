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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_REFERRER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_TITLE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_URL_FIELD;

public class AnalyticsEventPageDataTests extends AbstractWireSerializingTestCase<AnalyticsEventPageData> {

    public static AnalyticsEventPageData randomEventPageData() {
        return new AnalyticsEventPageData(randomIdentifier(), randomIdentifier(), randomIdentifier());
    }

    public void testToXContentWithAllFields() throws IOException {
        AnalyticsEventPageData page = randomEventPageData();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            BytesReference json = BytesReference.bytes(page.toXContent(builder, ToXContent.EMPTY_PARAMS));

            // Check the content that have been processed.
            Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

            assertEquals(3, contentAsMap.size());

            assertTrue(contentAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
            assertEquals(page.url(), contentAsMap.get(PAGE_URL_FIELD.getPreferredName()));

            assertTrue(contentAsMap.containsKey(PAGE_TITLE_FIELD.getPreferredName()));
            assertEquals(page.title(), contentAsMap.get(PAGE_TITLE_FIELD.getPreferredName()));

            assertTrue(contentAsMap.containsKey(PAGE_REFERRER_FIELD.getPreferredName()));
            assertEquals(page.referrer(), contentAsMap.get(PAGE_REFERRER_FIELD.getPreferredName()));

            // Check we can reserialize with fromXContent and object are equals
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
                assertEquals(page, AnalyticsEventPageData.fromXContent(parser, null));
            }
        }
    }

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventPageData page = new AnalyticsEventPageData(randomIdentifier(), null, null);
        BytesReference json = BytesReference.bytes(page.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(page.url(), contentAsMap.get(PAGE_URL_FIELD.getPreferredName()));

        // Check we can reserialize with fromXContent and object are equals
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array());
        assertEquals(page, AnalyticsEventPageData.fromXContent(parser, null));
    }

    public void testFromXContentWithAllFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .put(PAGE_TITLE_FIELD.getPreferredName(), randomIdentifier())
            .put(PAGE_REFERRER_FIELD.getPreferredName(), randomIdentifier())
            .map();

        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            AnalyticsEventPageData page = AnalyticsEventPageData.fromXContent(parser, null);

            assertEquals(jsonMap.get(PAGE_URL_FIELD.getPreferredName()), page.url());
            assertEquals(jsonMap.get(PAGE_TITLE_FIELD.getPreferredName()), page.title());
            assertEquals(jsonMap.get(PAGE_REFERRER_FIELD.getPreferredName()), page.referrer());
        }
    }

    public void testFromXContentWithOnlyRequiredFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .map();

        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            AnalyticsEventPageData page = AnalyticsEventPageData.fromXContent(parser, null);

            assertEquals(jsonMap.get(PAGE_URL_FIELD.getPreferredName()), page.url());
            assertNull(page.title());
            assertNull(page.referrer());
        }
    }

    public void testFromXContentWhenUrlFieldIsMissing() throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, "{}")) {
            expectThrows(
                IllegalArgumentException.class,
                LoggerMessageFormat.format("Required [{}]", PAGE_URL_FIELD.getPreferredName()),
                () -> AnalyticsEventPageData.fromXContent(parser, null)
            );
        }
    }

    public void testFromXContentWithAnInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}}] failed to parse field [{}]", PAGE_FIELD, invalidFieldName),
                () -> AnalyticsEventPageData.fromXContent(parser, null)
            );
        }
    }

    @Override
    protected Writeable.Reader<AnalyticsEventPageData> instanceReader() {
        return AnalyticsEventPageData::new;
    }

    @Override
    protected AnalyticsEventPageData createTestInstance() {
        return randomEventPageData();
    }

    @Override
    protected AnalyticsEventPageData mutateInstance(AnalyticsEventPageData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
