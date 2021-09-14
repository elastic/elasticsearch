/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.calendars;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

public class ScheduledEventTests extends AbstractXContentTestCase<ScheduledEvent> {

    public static ScheduledEvent testInstance(String calendarId, @Nullable String eventId) {
        int duration = randomIntBetween(1, 10000) * 1000;
        Date start = new Date(randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999) - duration );
        Date end = new Date(start.getTime() + duration);

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
