/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.calendars.CalendarTests;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

public class PutCalendarActionRequestTests extends AbstractXContentSerializingTestCase<PutCalendarAction.Request> {

    private final String calendarId = JobTests.randomValidJobId();

    @Override
    protected PutCalendarAction.Request createTestInstance() {
        return new PutCalendarAction.Request(CalendarTests.testInstance(calendarId));
    }

    @Override
    protected PutCalendarAction.Request mutateInstance(PutCalendarAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<PutCalendarAction.Request> instanceReader() {
        return PutCalendarAction.Request::new;
    }

    @Override
    protected PutCalendarAction.Request doParseInstance(XContentParser parser) {
        return PutCalendarAction.Request.parseRequest(calendarId, parser);
    }
}
