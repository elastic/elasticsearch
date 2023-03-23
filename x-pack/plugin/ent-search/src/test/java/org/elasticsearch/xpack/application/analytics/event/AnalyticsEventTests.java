/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class AnalyticsEventTests extends AbstractWireSerializingTestCase<AnalyticsEvent> {
    @SuppressWarnings("unchecked")
    public void testGetPayloadAsMap() {
        String payloadJson = """
                {"page": { "url": "URL" }, "session": { "id" : "SESSION_ID" } }
            """;
        AnalyticsContext context = mock(AnalyticsContext.class);
        AnalyticsEvent event = new AnalyticsEvent(context, XContentType.JSON, new BytesArray(payloadJson));
        Map<String, Object> eventPayload = event.getPayloadAsMap();

        assertTrue(eventPayload.containsKey("page"));
        Map<String, Object> pageData = (Map<String, Object>) eventPayload.get("page");
        assertTrue(pageData.containsKey("url"));
        assertEquals(pageData.get("url"), "URL");

        assertTrue(eventPayload.containsKey("session"));
        Map<String, Object> sessionData = (Map<String, Object>) eventPayload.get("session");
        assertTrue(sessionData.containsKey("id"));
        assertEquals(sessionData.get("id"), "SESSION_ID");
    }

    @Override
    protected Writeable.Reader<AnalyticsEvent> instanceReader() {
        return AnalyticsEvent::new;
    }

    @Override
    protected AnalyticsEvent createTestInstance() {
        AnalyticsContext analyticsContext = new AnalyticsContext(
            new AnalyticsCollection(randomIdentifier()),
            randomFrom(AnalyticsEventType.values()),
            randomLong()
        );
        return new AnalyticsEvent(analyticsContext, randomFrom(XContentType.values()), new BytesArray(randomIdentifier()));
    }

    @Override
    protected AnalyticsEvent mutateInstance(AnalyticsEvent instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
