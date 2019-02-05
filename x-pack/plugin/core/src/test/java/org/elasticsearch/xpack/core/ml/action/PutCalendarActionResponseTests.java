/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.client.ml.PutCalendarResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.CalendarTests;

import java.io.IOException;

public class PutCalendarActionResponseTests
    extends AbstractHlrcStreamableXContentTestCase<PutCalendarAction.Response, PutCalendarResponse> {

    @Override
    protected PutCalendarAction.Response createTestInstance() {
        return new PutCalendarAction.Response(CalendarTests.testInstance());
    }

    @Override
    protected PutCalendarAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutCalendarAction.Response(Calendar.LENIENT_PARSER.parse(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    public PutCalendarResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return PutCalendarResponse.fromXContent(parser);
    }

    @Override
    public PutCalendarAction.Response convertHlrcToInternal(PutCalendarResponse instance) {
        org.elasticsearch.client.ml.calendars.Calendar hlrcCalendar = instance.getCalendar();
        Calendar internalCalendar = new Calendar(hlrcCalendar.getId(), hlrcCalendar.getJobIds(), hlrcCalendar.getDescription());
        return new PutCalendarAction.Response(internalCalendar);
    }

    @Override
    protected PutCalendarAction.Response createBlankInstance() {
        return new PutCalendarAction.Response();
    }
}
