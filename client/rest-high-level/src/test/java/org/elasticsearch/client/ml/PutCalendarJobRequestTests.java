/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

public class PutCalendarJobRequestTests extends ESTestCase {

    public void testWithNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class,
            () -> new PutCalendarJobRequest(null, "job1"));
        assertEquals("[calendar_id] must not be null.", ex.getMessage());
    }

    public void testSetJobIds() {
        String calendarId = randomAlphaOfLength(10);

        NullPointerException ex = expectThrows(NullPointerException.class,
            () ->new PutCalendarJobRequest(calendarId, "job1", null));
        assertEquals("jobIds must not contain null values.", ex.getMessage());

        IllegalArgumentException illegalArgumentException =
            expectThrows(IllegalArgumentException.class, () -> new PutCalendarJobRequest(calendarId));
        assertEquals("jobIds must not be empty.", illegalArgumentException.getMessage());
    }
}
