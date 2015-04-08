/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.watcher.trigger.schedule.support.WeekTimes;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class WeeklyScheduleTests extends ScheduleTestCase {

    @Test
    public void test_Default() throws Exception {
        WeeklySchedule schedule = new WeeklySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 ? * MON"));
    }

    @Test @Repeat(iterations = 20)
    public void test_SingleTime() throws Exception {
        WeekTimes time = validWeekTime();
        WeeklySchedule schedule = new WeeklySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(time.times().length));
        for (DayTimes dayTimes : time.times()) {
            assertThat(crons, hasItemInArray("0 " + Ints.join(",", dayTimes.minute()) + " " + Ints.join(",", dayTimes.hour()) + " ? * " + Joiner.on(",").join(time.days())));
        }
    }

    @Test @Repeat(iterations = 20)
    public void test_MultipleTimes() throws Exception {
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
                assertThat(crons, hasItemInArray("0 " + Ints.join(",", dayTimes.minute()) + " " + Ints.join(",", dayTimes.hour()) + " ? * " + Joiner.on(",").join(weekTimes.days())));
            }
        }
    }

    @Test
    public void testParser_Empty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new WeekTimes(DayOfWeek.MONDAY, new DayTimes())));
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SingleTime() throws Exception {
        DayTimes time = validDayTime();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("on", "mon")
                .startObject("at")
                .field("hour", time.hour())
                .field("minute", time.minute())
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0].days(), hasSize(1));
        assertThat(schedule.times()[0].days(), contains(DayOfWeek.MONDAY));
        assertThat(schedule.times()[0].times(), arrayWithSize(1));
        assertThat(schedule.times()[0].times(), hasItemInArray(time));
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_SingleTime_Invalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("on", "mon")
                .startObject("at")
                .field("hour", time.hour)
                .field("minute", time.minute)
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new WeeklySchedule.Parser().parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_MultipleTimes() throws Exception {
        WeekTimes[] times = validWeekTimes();
        XContentBuilder builder = jsonBuilder().value(times);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        WeeklySchedule schedule = new WeeklySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (int i = 0; i < times.length; i++) {
            assertThat(schedule.times(), hasItemInArray(times[i]));
        }
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_MultipleTimes_Objects_Invalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("on", randomDaysOfWeek())
                .array("at", (Object[]) times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new WeeklySchedule.Parser().parse(parser);
    }
}
