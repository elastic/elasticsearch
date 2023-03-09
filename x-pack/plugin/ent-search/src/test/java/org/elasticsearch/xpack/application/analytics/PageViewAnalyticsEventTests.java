/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PageViewAnalyticsEventTests extends ESTestCase {
    private PageViewAnalyticsEvent pageViewAnalyticsEvent;

    @Before
    public void initializePageViewAnalyticsEvent() {
        pageViewAnalyticsEvent = new PageViewAnalyticsEvent(
            "test-collection",
            "https://test-url.com",
            "https://test-referrer.com",
            "test-event-source",
            Arrays.asList(Map.of("name", "test-name", "value", "test-value", "type", "test-type")),
            "test-user-uuid",
            "test-session-uuid"
        );
    }

    public void testFormattedEventHasRequiredFields() {
        ESLogMessage eventESLogMessage = pageViewAnalyticsEvent.toESLogMessage(new HashMap<>());

        assertEquals(eventESLogMessage.get("event.name"), "pageView");
        assertEquals(eventESLogMessage.get("event.source"), "test-event-source");
        assertEquals(eventESLogMessage.get("event.url"), "https://test-url.com");
        assertEquals(eventESLogMessage.get("event.referrer"), "https://test-referrer.com");
        assertEquals(eventESLogMessage.get("labels.fields"), "[{name=test-name, type=test-type, value=test-value}]");
        assertEquals(eventESLogMessage.get("labels.user_uuid"), "test-user-uuid");
        assertEquals(eventESLogMessage.get("labels.session_uuid"), "test-session-uuid");
    }

    public void testCollectionName() {
        assertEquals(pageViewAnalyticsEvent.getCollectionName(), "test-collection");
    }
}
