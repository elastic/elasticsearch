/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.PageViewAnalyticsEventTests.randomPageViewEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.SearchAnalyticsEventTests.randomSearchEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.SearchClickAnalyticsEventTests.randomSearchClickEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.CLIENT_ADDRESS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.USER_AGENT_FIELD;

public class AnalyticsEventFactoryTests extends ESTestCase {

    public void testFromPageViewEventRequest() throws IOException {
        AnalyticsEvent event = randomPageViewEvent();
        PostAnalyticsEventAction.Request request = toRequest(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromRequest(request));
    }

    public void testFromSearchEventRequest() throws IOException {
        AnalyticsEvent event = randomSearchEvent();
        PostAnalyticsEventAction.Request request = toRequest(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromRequest(request));
    }

    public void testFromSearchClickEventRequest() throws IOException {
        AnalyticsEvent event = randomSearchClickEvent();
        PostAnalyticsEventAction.Request request = toRequest(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromRequest(request));
    }

    public void testFromPayloadPageViewEvent() throws IOException {
        AnalyticsEvent event = randomPageViewEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, event.payload()));
    }

    private static PostAnalyticsEventAction.Request toRequest(AnalyticsEvent event) throws IOException {
        final PostAnalyticsEventAction.RequestBuilder requestBuilder = PostAnalyticsEventAction.Request.builder(
            event.eventCollectionName(),
            event.eventType().toString(),
            XContentType.JSON,
            toRequestPayload(event)
        );

        requestBuilder.eventTime(event.eventTime()).debug(randomBoolean());

        @SuppressWarnings("unchecked")
        Map<String, String> eventSessionData = (Map<String, String>) event.payloadAsMap().get(SESSION_FIELD.getPreferredName());

        Map<String, List<String>> headers = new HashMap<>();
        requestBuilder.clientAddress(eventSessionData.get(CLIENT_ADDRESS_FIELD.getPreferredName()));

        if (eventSessionData.containsKey(USER_AGENT_FIELD.getPreferredName())) {
            headers.put("User-Agent", List.of(eventSessionData.get(USER_AGENT_FIELD.getPreferredName())));
        }

        requestBuilder.headers(headers);
        return requestBuilder.request();
    }

    private static BytesReference toRequestPayload(AnalyticsEvent event) throws IOException {
        MapBuilder<String, Object> requestPayloadBuilder = MapBuilder.newMapBuilder(event.payloadAsMap());

        @SuppressWarnings("unchecked")
        Map<String, String> eventSessionData = (Map<String, String>) event.payloadAsMap().get(SESSION_FIELD.getPreferredName());

        Map<String, Object> requestSessionData = eventSessionData.entrySet()
            .stream()
            .filter(e -> e.getKey() != CLIENT_ADDRESS_FIELD.getPreferredName())
            .filter(e -> e.getKey() != USER_AGENT_FIELD.getPreferredName())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        requestPayloadBuilder.put(SESSION_FIELD.getPreferredName(), requestSessionData);

        return convertMapToJson(requestPayloadBuilder.map());
    }

    public void testFromPayloadSearchClickEvent() throws IOException {
        AnalyticsEvent event = randomSearchClickEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, event.payload()));
    }

    public void testFromPayloadSearchEvent() throws IOException {
        AnalyticsEvent event = randomSearchEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        final AnalyticsEvent actual = AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, event.payload());
        assertEquals(event, actual);
    }
}
