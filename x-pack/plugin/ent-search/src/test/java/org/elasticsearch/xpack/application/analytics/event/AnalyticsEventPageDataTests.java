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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_REFERRER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_TITLE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_URL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventPageData;

public class AnalyticsEventPageDataTests extends AbstractWireSerializingTestCase<AnalyticsEventPageData> {

    public void testToXContentWithAllFields() throws IOException {
        AnalyticsEventPageData page = randomEventPageData();

        // Serialize the page.
        BytesReference json = XContentHelper.toXContent(page, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

        assertEquals(3, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(page.url(), contentAsMap.get(PAGE_URL_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(PAGE_TITLE_FIELD.getPreferredName()));
        assertEquals(page.title(), contentAsMap.get(PAGE_TITLE_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(PAGE_REFERRER_FIELD.getPreferredName()));
        assertEquals(page.referrer(), contentAsMap.get(PAGE_REFERRER_FIELD.getPreferredName()));

        // Check we can re-serialize with fromXContent and object are equals.
        assertEquals(page, parsePageEventData(json));
    }

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventPageData page = new AnalyticsEventPageData(randomIdentifier(), null, null);

        // Serialize the page.
        BytesReference json = XContentHelper.toXContent(page, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(page.url(), contentAsMap.get(PAGE_URL_FIELD.getPreferredName()));

        // Check we can re-serialize with fromXContent and object are equals.
        assertEquals(page, parsePageEventData(json));
    }

    public void testFromXContentWithAllFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .put(PAGE_TITLE_FIELD.getPreferredName(), randomIdentifier())
            .put(PAGE_REFERRER_FIELD.getPreferredName(), randomIdentifier())
            .map();

        AnalyticsEventPageData page = parsePageEventData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(PAGE_URL_FIELD.getPreferredName()), page.url());
        assertEquals(jsonMap.get(PAGE_TITLE_FIELD.getPreferredName()), page.title());
        assertEquals(jsonMap.get(PAGE_REFERRER_FIELD.getPreferredName()), page.referrer());
    }

    public void testFromXContentWithOnlyRequiredFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .map();

        AnalyticsEventPageData page = parsePageEventData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(PAGE_URL_FIELD.getPreferredName()), page.url());
        assertNull(page.title());
        assertNull(page.referrer());
    }

    public void testFromXContentWhenUrlFieldIsMissing() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", PAGE_URL_FIELD.getPreferredName()),
            () -> parsePageEventData(new BytesArray("{}"))
        );
    }

    public void testFromXContentWithAnInvalidField() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_URL_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", PAGE_FIELD, invalidFieldName),
            () -> parsePageEventData(convertMapToJson(jsonMap))
        );
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

    private AnalyticsEventPageData parsePageEventData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventPageData.fromXContent(contentParser, null);
        }
    }
}
