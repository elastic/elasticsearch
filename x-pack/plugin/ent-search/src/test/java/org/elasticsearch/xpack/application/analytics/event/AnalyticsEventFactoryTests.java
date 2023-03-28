/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;

import java.io.IOException;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.PAGEVIEW;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.SEARCH;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.SEARCH_CLICK;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createPayloadFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomPageViewEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomSearchClickEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomSearchEvent;

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

    public void testFromStreamInputPageViewEvent() throws IOException {
        AnalyticsEvent event = randomPageViewEvent();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            event.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(event, AnalyticsEventFactory.INSTANCE.fromStreamInput(PAGEVIEW, in));
            }
        }
    }

    public void testFromStreamInputSearchEvent() throws IOException {
        AnalyticsEvent event = randomSearchEvent();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            event.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(event, AnalyticsEventFactory.INSTANCE.fromStreamInput(SEARCH, in));
            }
        }
    }

    public void testFromStreamInputSearchClickEvent() throws IOException {
        AnalyticsEvent event = randomSearchClickEvent();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            event.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(event, AnalyticsEventFactory.INSTANCE.fromStreamInput(SEARCH_CLICK, in));
            }
        }
    }

    public void testFromPayloadPageViewEvent() throws IOException {
        AnalyticsEvent event = randomPageViewEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        BytesReference payload = createPayloadFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, payload));
    }

    public void testFromPayloadSearchEvent() throws IOException {
        AnalyticsEvent event = randomSearchEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        BytesReference payload = createPayloadFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, payload));
    }

    public void testFromPayloadSearchClickEvent() throws IOException {
        AnalyticsEvent event = randomSearchClickEvent();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);
        BytesReference payload = createPayloadFromEvent(event);
        assertEquals(event, AnalyticsEventFactory.INSTANCE.fromPayload(context, XContentType.JSON, payload));
    }

    private PostAnalyticsEventAction.Request toRequest(AnalyticsEvent event) throws IOException {
        BytesReference payload = createPayloadFromEvent(event);

        return new PostAnalyticsEventAction.Request(
            event.eventCollectionName(),
            event.eventType().toString(),
            randomBoolean(),
            event.eventTime(),
            XContentType.JSON,
            payload
        );
    }
}
