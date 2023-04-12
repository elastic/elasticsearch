/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;

import java.io.IOException;

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

    public void testFromPayloadSearchEvent() throws IOException {
        AnalyticsEvent event = randomSearchEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, event.payload()));
    }

    public void testFromPayloadSearchClickEvent() throws IOException {
        AnalyticsEvent event = randomSearchClickEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, event.payload()));
    }

    private PostAnalyticsEventAction.Request toRequest(AnalyticsEvent event) {
        return new PostAnalyticsEventAction.Request(
            event.eventCollectionName(),
            event.eventType().toString(),
            randomBoolean(),
            event.eventTime(),
            XContentType.JSON,
            event.payload()
        );
    }
}
