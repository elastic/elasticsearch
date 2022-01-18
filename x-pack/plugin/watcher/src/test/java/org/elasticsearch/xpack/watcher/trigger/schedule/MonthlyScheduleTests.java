/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.MonthTimes;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Strings.join;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MonthlyScheduleTests extends ScheduleTestCase {
    public void testDefault() throws Exception {
        MonthlySchedule schedule = new MonthlySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 1 * ?"));
    }

    public void testSingleTime() throws Exception {
        MonthTimes time = validMonthTime();
        MonthlySchedule schedule = new MonthlySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(time.times().length));
        for (DayTimes dayTimes : time.times()) {
            String minStr = join(",", dayTimes.minute());
            String hrStr = join(",", dayTimes.hour());
            String dayStr = join(",", time.days());
            dayStr = dayStr.replace("32", "L");
            assertThat(crons, hasItemInArray("0 " + minStr + " " + hrStr + " " + dayStr + " * ?"));
        }
    }

    public void testMultipleTimes() throws Exception {
        MonthTimes[] times = validMonthTimes();
        MonthlySchedule schedule = new MonthlySchedule(times);
        String[] crons = expressions(schedule);
        int count = 0;
        for (MonthTimes time : times) {
            count += time.times().length;
        }
        assertThat(crons, arrayWithSize(count));
        for (MonthTimes monthTimes : times) {
            for (DayTimes dayTimes : monthTimes.times()) {
                String minStr = join(",", dayTimes.minute());
                String hrStr = join(",", dayTimes.hour());
                String dayStr = join(",", monthTimes.days());
                dayStr = dayStr.replace("32", "L");
                assertThat(crons, hasItemInArray("0 " + minStr + " " + hrStr + " " + dayStr + " * ?"));
            }
        }
    }

    public void testParserEmpty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        MonthlySchedule schedule = new MonthlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new MonthTimes()));
    }

    public void testParserSingleTime() throws Exception {
        DayTimes time = validDayTime();
        Object day = randomDayOfMonth();
        XContentBuilder builder = jsonBuilder().startObject()
            .field("on", day)
            .startObject("at")
            .array("hour", time.hour())
            .array("minute", time.minute())
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        MonthlySchedule schedule = new MonthlySchedule.Parser().parse(parser);
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
        MonthTimes[] times = validMonthTimes();
        XContentBuilder builder = jsonBuilder().value(times);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        MonthlySchedule schedule = new MonthlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (MonthTimes time : times) {
            assertThat(schedule.times(), hasItemInArray(time));
        }
    }

    public void testParserMultipleTimesInvalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder().startObject().field("on", randomDayOfMonth()).array("at", (Object[]) times).endObject();
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
}
