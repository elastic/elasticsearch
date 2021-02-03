/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.CalendarTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetCalendarsResponseTests extends AbstractXContentTestCase<GetCalendarsResponse> {

    @Override
    protected GetCalendarsResponse createTestInstance() {
        List<Calendar> calendars = new ArrayList<>();
        int count = randomIntBetween(0, 3);
        for (int i=0; i<count; i++) {
            calendars.add(CalendarTests.testInstance());
        }
        return new GetCalendarsResponse(calendars, count);
    }

    @Override
    protected GetCalendarsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetCalendarsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
