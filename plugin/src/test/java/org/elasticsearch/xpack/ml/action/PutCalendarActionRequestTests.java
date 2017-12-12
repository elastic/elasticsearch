/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.calendars.Calendar;

import java.util.ArrayList;
import java.util.List;

public class PutCalendarActionRequestTests extends AbstractStreamableXContentTestCase<PutCalendarAction.Request> {

    private final String calendarId = randomAlphaOfLengthBetween(1, 20);

    @Override
    protected PutCalendarAction.Request createTestInstance() {
        int size = randomInt(10);
        List<String> jobIds = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            jobIds.add(randomAlphaOfLengthBetween(1, 20));
        }
        Calendar calendar = new Calendar(calendarId, jobIds);
        return new PutCalendarAction.Request(calendar);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutCalendarAction.Request createBlankInstance() {
        return new PutCalendarAction.Request();
    }

    @Override
    protected PutCalendarAction.Request doParseInstance(XContentParser parser) {
        return PutCalendarAction.Request.parseRequest(calendarId, parser);
    }
}
