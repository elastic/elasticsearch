/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.AnalyticsTemplateRegistry;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.DATA_STREAM_DATASET_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.DATA_STREAM_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.DATA_STREAM_NAMESPACE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.DATA_STREAM_TYPE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.EVENT_ACTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.EVENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.TIMESTAMP_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_ID_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createPayloadFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_ID_FIELD;

public abstract class AbstractEventTestCase<T extends AnalyticsEvent> extends AbstractWireSerializingTestCase<T> {

    @SuppressWarnings("unchecked")
    public void testToXContentBaseEventFields() throws IOException {
        // Create a random event.
        T event = createTestInstance();

        // Serialize the event.
        BytesReference json = XContentHelper.toXContent(event, XContentType.JSON, false);

        // Convert JSON to a map, so we can easily assert on events.
        Map<String, Object> eventMap = XContentHelper.convertToMap(json, false, XContentType.JSON).v2();

        // Checking the event map has a timestamp.
        assertEquals(eventMap.get(TIMESTAMP_FIELD.getPreferredName()), event.eventTime());

        // Check the event map has data stream info.
        assertTrue(eventMap.containsKey(DATA_STREAM_FIELD.getPreferredName()));
        Map<String, String> dataStreamInfo = (Map<String, String>) eventMap.get(DATA_STREAM_FIELD.getPreferredName());
        assertEquals(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_TYPE, dataStreamInfo.get(DATA_STREAM_TYPE_FIELD.getPreferredName()));
        assertEquals(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_DATASET, dataStreamInfo.get(DATA_STREAM_DATASET_FIELD.getPreferredName()));
        assertEquals(event.eventCollectionName(), dataStreamInfo.get(DATA_STREAM_NAMESPACE_FIELD.getPreferredName()));

        // Check the event has a type
        assertTrue(eventMap.containsKey(EVENT_FIELD.getPreferredName()));
        Map<String, String> eventInfo = (Map<String, String>) eventMap.get(EVENT_FIELD.getPreferredName());
        assertEquals(eventInfo.get(EVENT_ACTION_FIELD.getPreferredName()), event.eventType().toString());

        // Check session id is rendered correctly
        assertTrue(eventMap.containsKey(SESSION_FIELD.getPreferredName()));
        Map<String, String> sessionData = (Map<String, String>) eventMap.get(SESSION_FIELD.getPreferredName());
        assertEquals(sessionData.get(SESSION_ID_FIELD.getPreferredName()), event.session().id());

        // Check user id is rendered correctly
        assertTrue(eventMap.containsKey(USER_FIELD.getPreferredName()));
        Map<String, String> userData = (Map<String, String>) eventMap.get(USER_FIELD.getPreferredName());
        assertEquals(userData.get(USER_ID_FIELD.getPreferredName()), event.user().id());

        assertToXContentAdditionalFields(eventMap, event);
    }

    public void testFromXContentFailsWhenSessionDataAreMissing() throws IOException {
        AnalyticsEvent event = createTestInstance();
        BytesReference payload = createPayloadFromEvent(event, SESSION_FIELD.getPreferredName());

        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);

        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", SESSION_FIELD.getPreferredName()),
            () -> parsePageEventData(context, payload)
        );
    }

    public void testFromXContentFailsWhenUserDataAreMissing() throws IOException {
        AnalyticsEvent event = createTestInstance();
        BytesReference payload = createPayloadFromEvent(event, USER_FIELD.getPreferredName());

        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);

        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", USER_FIELD.getPreferredName()),
            () -> parsePageEventData(context, payload)
        );
    }

    protected abstract ContextParser<AnalyticsEvent.Context, T> parser();

    protected abstract void assertToXContentAdditionalFields(Map<String, Object> eventMap, T event);

    protected T parsePageEventData(AnalyticsEvent.Context context, BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return parser().parse(contentParser, context);
        }
    }

}
