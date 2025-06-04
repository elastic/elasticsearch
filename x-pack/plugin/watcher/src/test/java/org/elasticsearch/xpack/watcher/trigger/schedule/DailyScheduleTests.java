/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.support.Strings.join;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DailyScheduleTests extends ScheduleTestCase {
    public void testDefault() throws Exception {
        DailySchedule schedule = new DailySchedule();
        String[] crons = expressions(schedule.crons());
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 0 * * ?"));
    }

    public void testSingleTime() throws Exception {
        DayTimes time = validDayTime();
        DailySchedule schedule = new DailySchedule(time);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + join(",", time.minute()) + " " + join(",", time.hour()) + " * * ?"));
    }

    public void testSingleTimeInvalid() throws Exception {
        HourAndMinute ham = invalidDayTime();
        try {
            new DayTimes(ham.hour, ham.minute);
            fail("expected an illegal argument exception on invalid time input");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("invalid time ["));
            assertThat(e.getMessage(), either(containsString("invalid time hour value")).or(containsString("invalid time minute value")));
        }
    }

    public void testMultipleTimes() throws Exception {
        DayTimes[] times = validDayTimes();
        DailySchedule schedule = new DailySchedule(times);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(times.length));
        for (DayTimes time : times) {
            assertThat(crons, hasItemInArray("0 " + join(",", time.minute()) + " " + join(",", time.hour()) + " * * ?"));
        }
    }

    public void testParserEmpty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(new DayTimes(0, 0)));
    }

    public void testParserSingleTimeObject() throws Exception {
        DayTimes time = validDayTime();
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("at")
            .array("hour", time.hour())
            .array("minute", time.minute())
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(time));
    }

    public void testParserSingleTimeObjectInvalid() throws Exception {
        HourAndMinute time = invalidDayTime();
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("at")
            .field("hour", time.hour)
            .field("minute", time.minute)
            .endObject()
            .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new DailySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [daily] schedule. invalid time value for field [at] - [START_OBJECT]"));
        }
    }

    public void testParserSingleTimeString() throws Exception {
        String timeStr = validDayTimeStr();
        XContentBuilder builder = jsonBuilder().startObject().field("at", timeStr).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(1));
        assertThat(schedule.times()[0], is(DayTimes.parse(timeStr)));
    }

    public void testParserSingleTimeStringInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("at", invalidDayTimeStr()).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new DailySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [daily] schedule. invalid time value for field [at] - [VALUE_STRING]"));
        }
    }

    public void testParserMultipleTimesObjects() throws Exception {
        DayTimes[] times = validDayTimesFromNumbers();
        XContentBuilder builder = jsonBuilder().startObject().array("at", (Object[]) times).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (int i = 0; i < times.length; i++) {
            assertThat(schedule.times(), hasItemInArray(times[i]));
        }
    }

    public void testParserMultipleTimesObjectsInvalid() throws Exception {
        HourAndMinute[] times = invalidDayTimes();
        XContentBuilder builder = jsonBuilder().startObject().array("at", (Object[]) times).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new DailySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [daily] schedule. invalid time value for field [at] - [START_OBJECT]"));
        }
    }

    public void testParserMultipleTimesStrings() throws Exception {
        DayTimes[] times = validDayTimesFromStrings();
        XContentBuilder builder = jsonBuilder().startObject().array("at", (Object[]) times).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        DailySchedule schedule = new DailySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.times().length, is(times.length));
        for (int i = 0; i < times.length; i++) {
            assertThat(schedule.times(), hasItemInArray(times[i]));
        }
    }

    public void testParserMultipleTimesStringsInvalid() throws Exception {
        String[] times = invalidDayTimesAsStrings();
        XContentBuilder builder = jsonBuilder().startObject().array("at", times).endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new DailySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [daily] schedule. invalid time value for field [at] - [VALUE_STRING]"));
        }
    }
}
