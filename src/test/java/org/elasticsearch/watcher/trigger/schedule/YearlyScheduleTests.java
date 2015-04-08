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
import org.elasticsearch.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.watcher.trigger.schedule.support.YearTimes;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class YearlyScheduleTests extends ScheduleTestCase {

    @Test
    public void test_Default() throws Exception {
        YearlySchedule schedule = new YearlySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 1 JAN ?"));
    }

    @Test @Repeat(iterations = 120)
    public void test_SingleTime() throws Exception {
        YearTimes time = validYearTime();
        YearlySchedule schedule = new YearlySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(time.times().length));
        for (DayTimes dayTimes : time.times()) {
            String minStr = Ints.join(",", dayTimes.minute());
            String hrStr = Ints.join(",", dayTimes.hour());
            String dayStr = Ints.join(",", time.days());
            dayStr = dayStr.replace("32", "L");
            String monthStr = Joiner.on(",").join(time.months());
            String expression = "0 " + minStr + " " + hrStr + " " + dayStr + " " + monthStr + " ?";
            logger.info("expression: " + expression);
            assertThat(crons, hasItemInArray(expression));
        }
    }

    @Test @Repeat(iterations = 20)
    public void test_MultipleTimes() throws Exception {
        YearTimes[] times = validYearTimes();
        YearlySchedule schedule = new YearlySchedule(times);
        String[] crons = expressions(schedule);
        int count = 0;
        for (int i = 0; i < times.length; i++) {
            count += times[i].times().length;
        }
        assertThat(crons, arrayWithSize(count));
        for (YearTimes yearTimes : times) {
            for (DayTimes dayTimes : yearTimes.times()) {
                String minStr = Ints.join(",", dayTimes.minute());
                String hrStr = Ints.join(",", dayTimes.hour());
                String dayStr = Ints.join(",", yearTimes.days());
                dayStr = dayStr.replace("32", "L");
                String monthStr = Joiner.on(",").join(yearTimes.months());
                assertThat(crons, hasItemInArray("0 " + minStr + " " + hrStr + " " + dayStr + " " + monthStr + " ?"));
            }
        }
    }

    @Test @Repeat(iterations = 20)
    public void testParser_Empty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new YearTimes()));
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SingleTime() throws Exception {
        DayTimes time = validDayTime();
        Object day = randomDayOfMonth();
        Object month = randomMonth();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("in", month)
                .field("on", day)
                .startObject("at")
                .field("hour", time.hour())
                .field("minute", time.minute())
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0].days().length, is(1));
        assertThat(schedule.times()[0].days()[0], is(dayOfMonthToInt(day)));
        assertThat(schedule.times()[0].times(), arrayWithSize(1));
        assertThat(schedule.times()[0].times(), hasItemInArray(time));
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_SingleTime_Invalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("in", randomMonth())
                .field("on", randomBoolean() ? invalidDayOfMonth() : randomDayOfMonth())
                .startObject("at")
                .field("hour", time.hour)
                .field("minute", time.minute)
                .endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new MonthlySchedule.Parser().parse(parser);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_MultipleTimes() throws Exception {
        YearTimes[] times = validYearTimes();
        XContentBuilder builder = jsonBuilder().value(times);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (YearTimes time : times) {
            assertThat(schedule.times(), hasItemInArray(time));
        }
    }

    @Test(expected = WatcherSettingsException.class) @Repeat(iterations = 20)
    public void testParser_MultipleTimes_Invalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("in", randomMonth())
                .field("on", randomDayOfMonth())
                .array("at", (Object[]) times)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new YearlySchedule.Parser().parse(parser);
    }
}
