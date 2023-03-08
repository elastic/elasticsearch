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

public class SearchAnalyticsEventTests extends ESTestCase {
    private SearchAnalyticsEvent searchEvent;

    @Before
    public void initializeSearchAnalyticsEvent() {
        searchEvent = new SearchAnalyticsEvent(
            "test-collection",
            "test-engine",
            "test-event-source",
            Arrays.asList(Map.of("name", "test-name", "value", "test-value", "type", "test-type")),
            new String[] { "test-item" },
            "test-user-uuid",
            "test-session-uuid"
        );
    }

    public void testFormattedEventHasRequiredFields() {
        ESLogMessage eventESLogMessage = searchEvent.toESLogMessage(new HashMap<>());
        assertEquals(eventESLogMessage.get("event.name"), "search");
        assertEquals(eventESLogMessage.get("event.source"), "test-event-source");
        assertEquals(eventESLogMessage.get("labels.engineName"), "test-engine");
        assertEquals(eventESLogMessage.get("labels.user_uuid"), "test-user-uuid");
        assertEquals(eventESLogMessage.get("labels.session_uuid"), "test-session-uuid");
        assertEquals(eventESLogMessage.get("labels.fields"), "[{name=test-name, type=test-type, value=test-value}]");
    }

    public void testCollectionName() {
        assertEquals(searchEvent.getCollectionName(), "test-collection");
    }
}
