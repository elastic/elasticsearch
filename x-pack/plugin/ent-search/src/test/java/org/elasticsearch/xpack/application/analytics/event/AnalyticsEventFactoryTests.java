/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.PageViewAnalyticsEventTests.randomPageViewEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.SearchAnalyticsEventTests.randomSearchEvent;
import static org.elasticsearch.xpack.application.analytics.event.parser.event.SearchClickAnalyticsEventTests.randomSearchClickEvent;

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

    private static PostAnalyticsEventAction.Request toRequest(AnalyticsEvent event) {
        final PostAnalyticsEventAction.RequestBuilder requestBuilder = PostAnalyticsEventAction.Request.builder(
            event.eventCollectionName(),
            event.eventType().toString(),
            XContentType.JSON,
            event.payload()
        ).eventTime(event.eventTime()).debug(randomBoolean()).clientAddress(event.clientAddress());
        Map<String, List<String>> headers = randomMap(
            0,
            5,
            () -> new Tuple<>(randomIdentifier(), randomList(1, 5, ESTestCase::randomIdentifier))
        );
        if (event.userAgent() != null) {
            headers.put("User-Agent", List.of(event.userAgent()));
        }
        requestBuilder.headers(headers);
        return requestBuilder.request();
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
