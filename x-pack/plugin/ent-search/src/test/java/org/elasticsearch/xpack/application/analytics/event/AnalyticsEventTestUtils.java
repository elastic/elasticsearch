/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.DATA_STREAM_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.EVENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.TIMESTAMP_FIELD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsEventTestUtils {
    public static BytesReference createPayloadFromEvent(AnalyticsEvent event, String... ignoredFields) throws IOException {
        // Serialize the event.
        BytesReference json = XContentHelper.toXContent(event, XContentType.JSON, false);

        // Convert JSON to a map, so we can easily assert on events.
        Map<String, Object> eventMap = XContentHelper.convertToMap(json, false, XContentType.JSON).v2();

        for (String field : ignoredFields) {
            eventMap.remove(field);
        }

        eventMap.remove(EVENT_FIELD.getPreferredName());
        eventMap.remove(DATA_STREAM_FIELD.getPreferredName());
        eventMap.remove(TIMESTAMP_FIELD.getPreferredName());

        try (XContentBuilder builder = JsonXContent.contentBuilder().map(eventMap)) {
            return BytesReference.bytes(builder);
        }
    }

    public static BytesReference convertMapToJson(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder);
        }
    }

    public static AnalyticsEvent.Context createAnalyticsContextMockFromEvent(AnalyticsEvent event) {
        AnalyticsEvent.Context context = mock(AnalyticsEvent.Context.class);

        when(context.eventCollectionName()).thenReturn(event.eventCollectionName());
        when(context.eventTime()).thenReturn(event.eventTime());
        when(context.eventType()).thenReturn(event.eventType());

        return context;
    }

    public static AnalyticsEventPageData randomEventPageData() {
        return new AnalyticsEventPageData(randomIdentifier(), randomIdentifier(), randomIdentifier());
    }

    public static AnalyticsEventSearchData randomEventSearchData() {
        return new AnalyticsEventSearchData(randomIdentifier());
    }

    public static AnalyticsEventSessionData randomEventSessionData() {
        return new AnalyticsEventSessionData(randomIdentifier());
    }

    public static AnalyticsEventUserData randomEventUserData() {
        return new AnalyticsEventUserData(randomIdentifier());
    }

    public static AnalyticsEventPageView randomPageViewEvent() {
        return new AnalyticsEventPageView(
            randomIdentifier(),
            randomLong(),
            randomEventSessionData(),
            randomEventUserData(),
            randomEventPageData()
        );
    }

    public static AnalyticsEventSearch randomSearchEvent() {
        return new AnalyticsEventSearch(
            randomIdentifier(),
            randomLong(),
            randomEventSessionData(),
            randomEventUserData(),
            randomEventSearchData()
        );
    }

    public static AnalyticsEventSearchClick randomSearchClickEvent() {
        return new AnalyticsEventSearchClick(
            randomIdentifier(),
            randomLong(),
            randomEventSessionData(),
            randomEventUserData(),
            randomEventPageData(),
            randomEventSearchData()
        );
    }
}
