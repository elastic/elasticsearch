/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.YearTimes;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Strings.join;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class YearlyScheduleTests extends ScheduleTestCase {
    public void testDefault() throws Exception {
        YearlySchedule schedule = new YearlySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 1 JAN ?"));
    }

    public void testSingleTime() throws Exception {
        YearTimes time = validYearTime();
        YearlySchedule schedule = new YearlySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(time.times().length));
        for (DayTimes dayTimes : time.times()) {
            String minStr = join(",", dayTimes.minute());
            String hrStr = join(",", dayTimes.hour());
            String dayStr = join(",", time.days());
            dayStr = dayStr.replace("32", "L");
            String monthStr = Strings.collectionToCommaDelimitedString(time.months());
            String expression = "0 " + minStr + " " + hrStr + " " + dayStr + " " + monthStr + " ?";
            logger.info("expression: {}", expression);
            assertThat(crons, hasItemInArray(expression));
        }
    }

    public void testMultipleTimes() throws Exception {
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
                String minStr = join(",", dayTimes.minute());
                String hrStr = join(",", dayTimes.hour());
                String dayStr = join(",", yearTimes.days());
                dayStr = dayStr.replace("32", "L");
                String monthStr = Strings.collectionToCommaDelimitedString(yearTimes.months());
                assertThat(crons, hasItemInArray("0 " + minStr + " " + hrStr + " " + dayStr + " " + monthStr + " ?"));
            }
        }
    }

    public void testParserEmpty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new YearTimes()));
    }

    public void testParserSingleTime() throws Exception {
        DayTimes time = validDayTime();
        Object day = randomDayOfMonth();
        Object month = randomMonth();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("in", month)
            .field("on", day)
            .startObject("at")
            .array("hour", time.hour())
            .array("minute", time.minute())
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0].days().length, is(1));
        assertThat(schedule.times()[0].days()[0], is(dayOfMonthToInt(day)));
        assertThat(schedule.times()[0].times(), arrayWithSize(1));
        assertThat(schedule.times()[0].times(), hasItemInArray(time));
    }

    public void testParserSingleTimeInvalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("in", randomMonth())
            .field("on", randomBoolean() ? invalidDayOfMonth() : randomDayOfMonth())
            .startObject("at")
            .field("hour", time.hour)
            .field("minute", time.minute)
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new MonthlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [monthly] schedule. invalid month times"));
        }
    }

    public void testParserMultipleTimes() throws Exception {
        YearTimes[] times = validYearTimes();
        XContentBuilder builder = jsonBuilder().value(times);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        YearlySchedule schedule = new YearlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (YearTimes time : times) {
            assertThat(schedule.times(), hasItemInArray(time));
        }
    }

    public void testParserMultipleTimesInvalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("in", randomMonth())
            .field("on", randomDayOfMonth())
            .array("at", (Object[]) times)
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new YearlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [yearly] schedule. invalid year times"));
        }
    }
}
