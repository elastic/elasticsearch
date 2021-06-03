/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.calendars;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

public class ScheduledEventTests extends AbstractXContentTestCase<ScheduledEvent> {

    public static ScheduledEvent testInstance(String calendarId, @Nullable String eventId) {
        Date start = new Date(randomNonNegativeLong());
        Date end = new Date(start.getTime() + randomIntBetween(1, 10000) * 1000);

        return new ScheduledEvent(randomAlphaOfLength(10), start, end, calendarId, eventId);
    }

    public static ScheduledEvent testInstance() {
        return testInstance(randomAlphaOfLengthBetween(1, 20),
            randomBoolean() ? null : randomAlphaOfLength(7));
    }

    @Override
    protected ScheduledEvent createTestInstance() {
        return testInstance();
    }

    @Override
    protected ScheduledEvent doParseInstance(XContentParser parser) {
        return ScheduledEvent.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
