/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.calendars.CalendarTests;

public class PutCalendarActionResponseTests extends AbstractStreamableTestCase<PutCalendarAction.Response> {

    @Override
    protected PutCalendarAction.Response createTestInstance() {
        return new PutCalendarAction.Response(CalendarTests.testInstance());
    }

    @Override
    protected PutCalendarAction.Response createBlankInstance() {
        return new PutCalendarAction.Response();
    }
}
