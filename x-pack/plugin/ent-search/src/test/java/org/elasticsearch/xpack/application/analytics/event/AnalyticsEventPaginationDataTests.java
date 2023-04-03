/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.CURRENT_PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventPaginationData;

public class AnalyticsEventPaginationDataTests extends AbstractWireSerializingTestCase<AnalyticsEventPaginationData> {

    public void testToXContent() throws IOException {
        AnalyticsEventPaginationData paginationData = randomEventPaginationData();

        // Serialize the paginationData data
        BytesReference json = XContentHelper.toXContent(paginationData, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(2, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(CURRENT_PAGE_FIELD.getPreferredName()));
        assertEquals(paginationData.current(), contentAsMap.get(CURRENT_PAGE_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(PAGE_SIZE_FIELD.getPreferredName()));
        assertEquals(paginationData.size(), contentAsMap.get(PAGE_SIZE_FIELD.getPreferredName()));

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(paginationData, parsePaginationData(json));
    }

    public void testFromXContent() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(CURRENT_PAGE_FIELD.getPreferredName(), randomNonNegativeInt())
            .put(PAGE_SIZE_FIELD.getPreferredName(), randomNonNegativeInt())
            .map();

        AnalyticsEventPaginationData paginationData = parsePaginationData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(CURRENT_PAGE_FIELD.getPreferredName()), paginationData.current());
        assertEquals(jsonMap.get(PAGE_SIZE_FIELD.getPreferredName()), paginationData.size());
    }

    public void testFromXContentWhenCurrentFieldIsMissing() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_SIZE_FIELD.getPreferredName(), randomNonNegativeInt())
            .map();

        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", CURRENT_PAGE_FIELD.getPreferredName()),
            () -> parsePaginationData(convertMapToJson(jsonMap))
        );
    }

    public void testFromXContentWhenSizeFieldIsMissing() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(CURRENT_PAGE_FIELD.getPreferredName(), randomNonNegativeInt())
            .map();

        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", PAGE_SIZE_FIELD.getPreferredName()),
            () -> parsePaginationData(convertMapToJson(jsonMap))
        );
    }

    public void testFromXContentWithAnInvalidField() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(CURRENT_PAGE_FIELD.getPreferredName(), randomNonNegativeInt())
            .put(PAGE_SIZE_FIELD.getPreferredName(), randomNonNegativeInt())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", PAGINATION_FIELD.getPreferredName(), invalidFieldName),
            () -> parsePaginationData(convertMapToJson(jsonMap))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventPaginationData> instanceReader() {
        return AnalyticsEventPaginationData::new;
    }

    @Override
    protected AnalyticsEventPaginationData createTestInstance() {
        return randomEventPaginationData();
    }

    @Override
    protected AnalyticsEventPaginationData mutateInstance(AnalyticsEventPaginationData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventPaginationData parsePaginationData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventPaginationData.fromXContent(contentParser, null);
        }
    }
}
