/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.calendars.CalendarTests;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

public class PutCalendarActionRequestTests extends AbstractSerializingTestCase<PutCalendarAction.Request> {

    private final String calendarId = JobTests.randomValidJobId();

    @Override
    protected PutCalendarAction.Request createTestInstance() {
        return new PutCalendarAction.Request(CalendarTests.testInstance(calendarId));
    }

    @Override
    protected Writeable.Reader<PutCalendarAction.Request> instanceReader() {
        return PutCalendarAction.Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutCalendarAction.Request doParseInstance(XContentParser parser) {
        return PutCalendarAction.Request.parseRequest(calendarId, parser);
    }
}
