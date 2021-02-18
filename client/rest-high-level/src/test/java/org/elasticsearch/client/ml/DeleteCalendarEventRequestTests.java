/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

public class DeleteCalendarEventRequestTests extends ESTestCase {

    public void testWithNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class,
            () -> new DeleteCalendarEventRequest(null, "event1"));
        assertEquals("[calendar_id] must not be null.", ex.getMessage());
    }

    public void testWithNullEvent() {
        NullPointerException ex = expectThrows(NullPointerException.class,
            () ->new DeleteCalendarEventRequest("calendarId", null));
        assertEquals("[event_id] must not be null.", ex.getMessage());
    }
}
