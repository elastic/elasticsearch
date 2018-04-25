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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1511395200000L), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1515369600000L), ZoneOffset.UTC))
                .calendarId("calendar_id").build());
        events.add(new ScheduledEvent.Builder().description("Blue Monday")
                .startTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1519603200000L), ZoneOffset.UTC))
                .endTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1519862400000L), ZoneOffset.UTC))
                .calendarId("calendar_id").build());

        StringBuilder buffer = new StringBuilder();
        new ScheduledEventsWriter(events, TimeValue.timeValueHours(1), buffer).write();

        String expectedString = "scheduledevent.0.description = Black Friday\n" +
                "scheduledevent.0.rules = [{\"actions\":[\"filter_results\",\"skip_sampling\"],\"conditions_connective\":\"and\"," +
                "\"conditions\":[{\"type\":\"time\",\"condition\":{\"operator\":\"gte\",\"value\":\"1511395200\"}}," +
                "{\"type\":\"time\",\"condition\":{\"operator\":\"lt\",\"value\":\"1515369600\"}}]}]\n" +
                "scheduledevent.1.description = Blue Monday\n" +
                "scheduledevent.1.rules = [{\"actions\":[\"filter_results\",\"skip_sampling\"],\"conditions_connective\":\"and\"," +
                "\"conditions\":[{\"type\":\"time\",\"condition\":{\"operator\":\"gte\",\"value\":\"1519603200\"}}," +
                "{\"type\":\"time\",\"condition\":{\"operator\":\"lt\",\"value\":\"1519862400\"}}]}]" +
                "\n";
        assertThat(buffer.toString(), equalTo(expectedString));
    }
}