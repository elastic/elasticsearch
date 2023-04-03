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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.DIRECTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.NAME_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.SORT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSortOrderData;

public class AnalyticsEventSortOrderDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSortOrderData> {

    public void testToXContentWithAllFields() throws IOException {
        AnalyticsEventSortOrderData sortOrder = randomEventSortOrderData();

        // Serialize the sortOrder data
        BytesReference json = XContentHelper.toXContent(sortOrder, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(2, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(NAME_FIELD.getPreferredName()));
        assertEquals(sortOrder.name(), contentAsMap.get(NAME_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(DIRECTION_FIELD.getPreferredName()));
        assertEquals(sortOrder.direction(), contentAsMap.get(DIRECTION_FIELD.getPreferredName()));

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(sortOrder, parseSortOrderData(json));
    }

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventSortOrderData sortOrder = new AnalyticsEventSortOrderData(randomIdentifier());

        // Serialize the sortOrder data
        BytesReference json = XContentHelper.toXContent(sortOrder, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(NAME_FIELD.getPreferredName()));
        assertEquals(sortOrder.name(), contentAsMap.get(NAME_FIELD.getPreferredName()));

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(sortOrder, parseSortOrderData(json));
    }

    public void testFromXContentWithAllFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(NAME_FIELD.getPreferredName(), randomIdentifier())
            .put(DIRECTION_FIELD.getPreferredName(), randomIdentifier())
            .map();

        AnalyticsEventSortOrderData sortOrder = parseSortOrderData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(NAME_FIELD.getPreferredName()), sortOrder.name());
        assertEquals(jsonMap.get(DIRECTION_FIELD.getPreferredName()), sortOrder.direction());
    }

    public void testFromXContentOnlyRequiredFields() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(NAME_FIELD.getPreferredName(), randomIdentifier())
            .map();

        AnalyticsEventSortOrderData sortOrder = parseSortOrderData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(NAME_FIELD.getPreferredName()), sortOrder.name());
        assertNull(sortOrder.direction());
    }

    public void testFromXContentWhenNameFieldIsMissing() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", NAME_FIELD.getPreferredName()),
            () -> parseSortOrderData(new BytesArray("{}"))
        );
    }

    public void testFromXContentWhenNameIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(NAME_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}] failed to parse field [{}]", SORT_FIELD.getPreferredName(), NAME_FIELD),
            () -> parseSortOrderData(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", NAME_FIELD), e.getCause().getMessage());
    }

    public void testFromXContentWithAnInvalidField() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(NAME_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SORT_FIELD.getPreferredName(), invalidFieldName),
            () -> parseSortOrderData(convertMapToJson(jsonMap))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSortOrderData> instanceReader() {
        return AnalyticsEventSortOrderData::new;
    }

    @Override
    protected AnalyticsEventSortOrderData createTestInstance() {
        return randomEventSortOrderData();
    }

    @Override
    protected AnalyticsEventSortOrderData mutateInstance(AnalyticsEventSortOrderData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventSortOrderData parseSortOrderData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSortOrderData.fromXContent(contentParser, null);
        }
    }
}
