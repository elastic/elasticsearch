/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.trigger.schedule.support.DayTimes;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class DailyScheduleTests extends ScheduleTestCase {

    @Test
    public void test_Default() throws Exception {
        DailySchedule schedule = new DailySchedule();
        String[] crons = expressions(schedule.crons());
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 * * ?"));
    }

    @Test @Repeat(iterations = 20)
    public void test_SingleTime() throws Exception {
        DayTimes time = validDayTime();
        DailySchedule schedule = new DailySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + Ints.join(",", time.minute()) + " " + Ints.join(",", time.hour()) + " * * ?"));
    }

    @Test @Repeat(iterations = 20)
    public void test_SingleTime_Invalid() throws Exception {
        try {
            HourAndMinute ham = invalidDayTime();
            new DayTimes(ham.hour, ham.minute);
            fail("expected either a parse exception or an watcher settings exception on invalid time input");
        } catch (DayTimes.ParseException pe) {
            // expected
        } catch (WatcherSettingsException ase) {
            // expected
        }
    }

    @Test @Repeat(iterations = 20)
    public void test_MultipleTimes() throws Exception {
        DayTimes[] times = validDayTimes();
        DailySchedule schedule = new DailySchedule(times);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(times.length));
        for (DayTimes time : times) {
            assertThat(crons, hasItemInArray("0 " + Ints.join(",", time.minute()) + " " + Ints.join(",", time.hour()) + " * * ?"));
        }
    }

    @Test
    public void testParser_Empty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new DayTimes(0, 0)));
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SingleTime_Object() throws Exception {
        DayTimes time = validDayTime();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("at")
                .field("hour", time.hour())
                .field("minute", time.minute())
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(time));
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_SingleTime_Object_Invalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject("at")
                .field("hour", time.hour)
                .field("minute", time.minute)
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new DailySchedule.Parser().parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SingleTime_String() throws Exception {
        String timeStr = validDayTimeStr();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("at", timeStr)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(DayTimes.parse(timeStr)));
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_SingleTime_String_Invalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("at", invalidDayTimeStr())
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new DailySchedule.Parser().parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_MultipleTimes_Objects() throws Exception {
        DayTimes[] times = validDayTimesFromNumbers();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .array("at", (Object[]) times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
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
                .array("at", (Object[]) times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new DailySchedule.Parser().parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_MultipleTimes_Strings() throws Exception {
        DayTimes[] times = validDayTimesFromStrings();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .array("at", (Object[]) times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (int i = 0; i < times.length; i++) {
            assertThat(schedule.times(), hasItemInArray(times[i]));
        }
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_MultipleTimes_Strings_Invalid() throws Exception {
        String[] times = invalidDayTimesAsStrings();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("at", times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new DailySchedule.Parser().parse(parser);
    }

}
