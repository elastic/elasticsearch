/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Strings.join;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class WeeklyScheduleTests extends ScheduleTestCase {
    public void testDefault() throws Exception {
        WeeklySchedule schedule = new WeeklySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 ? * MON"));
    }

    public void testSingleTime() throws Exception {
        WeekTimes time = validWeekTime();
        WeeklySchedule schedule = new WeeklySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(time.times().length));
        for (DayTimes dayTimes : time.times()) {
            assertThat(
                crons,
                hasItemInArray(
                    "0 "
                        + join(",", dayTimes.minute())
                        + " "
                        + join(",", dayTimes.hour())
                        + " ? * "
                        + Strings.collectionToCommaDelimitedString(time.days())
                )
            );
        }
    }

    public void testMultipleTimes() throws Exception {
        WeekTimes[] times = validWeekTimes();
        WeeklySchedule schedule = new WeeklySchedule(times);
        String[] crons = expressions(schedule);
        int count = 0;
        for (int i = 0; i < times.length; i++) {
            count += times[i].times().length;
        }
        assertThat(crons, arrayWithSize(count));
        for (WeekTimes weekTimes : times) {
            for (DayTimes dayTimes : weekTimes.times()) {
                assertThat(
                    crons,
                    hasItemInArray(
                        "0 "
                            + join(",", dayTimes.minute())
                            + " "
                            + join(",", dayTimes.hour())
                            + " ? * "
                            + Strings.collectionToCommaDelimitedString(weekTimes.days())
                    )
                );
            }
        }
    }

    public void testParserEmpty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new WeekTimes(DayOfWeek.MONDAY, new DayTimes())));
    }

    public void testParserSingleTime() throws Exception {
        DayTimes time = validDayTime();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("on", "mon")
            .startObject("at")
            .array("hour", time.hour())
            .array("minute", time.minute())
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0].days(), hasSize(1));
        assertThat(schedule.times()[0].days(), contains(DayOfWeek.MONDAY));
        assertThat(schedule.times()[0].times(), arrayWithSize(1));
        assertThat(schedule.times()[0].times(), hasItemInArray(time));
    }

    public void testParserSingleTimeInvalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("on", "mon")
            .startObject("at")
            .field("hour", time.hour)
            .field("minute", time.minute)
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new WeeklySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [weekly] schedule. invalid weekly times"));
        }
    }

    public void testParserMultipleTimes() throws Exception {
        WeekTimes[] times = validWeekTimes();
        XContentBuilder builder = jsonBuilder().value(times);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (int i = 0; i < times.length; i++) {
            assertThat(schedule.times(), hasItemInArray(times[i]));
        }
    }

    public void testParserMultipleTimesObjectsInvalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder().startObject().field("on", randomDaysOfWeek()).array("at", (Object[]) times).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new WeeklySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [weekly] schedule. invalid weekly times"));
        }
    }
}
