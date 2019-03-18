/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScheduledEventsWriterTests extends ESTestCase {

    public void testWrite_GivenEmpty() throws IOException {
        StringBuilder buffer = new StringBuilder();
        new ScheduledEventsWriter(Collections.emptyList(), TimeValue.timeValueHours(1), buffer).write();

        assertThat(buffer.toString().isEmpty(), is(true));
    }

    public void testWrite() throws IOException {
        List<ScheduledEvent> events = new ArrayList<>();
        events.add(new ScheduledEvent.Builder().description("Black Friday")
                .startTime(Instant.ofEpochMilli(1511395200000L))
                .endTime(Instant.ofEpochMilli(1515369600000L))
                .calendarId("calendar_id").build());
        events.add(new ScheduledEvent.Builder().description("Blue Monday")
                .startTime(Instant.ofEpochMilli(1519603200000L))
                .endTime(Instant.ofEpochMilli(1519862400000L))
                .calendarId("calendar_id").build());

        StringBuilder buffer = new StringBuilder();
        new ScheduledEventsWriter(events, TimeValue.timeValueHours(1), buffer).write();

        String expectedString = "scheduledevent.0.description = Black Friday\n" +
                "scheduledevent.0.rules = [{\"actions\":[\"skip_result\",\"skip_model_update\"]," +
                "\"conditions\":[{\"applies_to\":\"time\",\"operator\":\"gte\",\"value\":1.5113952E9}," +
                "{\"applies_to\":\"time\",\"operator\":\"lt\",\"value\":1.5153696E9}]}]\n" +
                "scheduledevent.1.description = Blue Monday\n" +
                "scheduledevent.1.rules = [{\"actions\":[\"skip_result\",\"skip_model_update\"]," +
                "\"conditions\":[{\"applies_to\":\"time\",\"operator\":\"gte\",\"value\":1.5196032E9}," +
                "{\"applies_to\":\"time\",\"operator\":\"lt\",\"value\":1.5198624E9}]}]" +
                "\n";
        assertThat(buffer.toString(), equalTo(expectedString));
    }
}
