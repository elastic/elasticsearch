/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.CalendarTests;

import java.io.IOException;

public class PutCalendarActionResponseTests extends AbstractStreamableXContentTestCase<PutCalendarAction.Response> {

    @Override
    protected PutCalendarAction.Response createTestInstance() {
        return new PutCalendarAction.Response(CalendarTests.testInstance());
    }

    @Override
    protected PutCalendarAction.Response createBlankInstance() {
        return new PutCalendarAction.Response();
    }

    @Override
    protected PutCalendarAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutCalendarAction.Response(Calendar.LENIENT_PARSER.parse(parser, null).build());
    }
}
