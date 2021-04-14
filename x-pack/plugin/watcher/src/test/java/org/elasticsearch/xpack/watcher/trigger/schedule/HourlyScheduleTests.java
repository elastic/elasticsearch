/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.watcher.support.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class HourlyScheduleTests extends ScheduleTestCase {
    public void testDefault() throws Exception {
        HourlySchedule schedule = new HourlySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 * * * ?"));
    }

    public void testSingleMinute() throws Exception {
        int minute = validMinute();
        HourlySchedule schedule = new HourlySchedule(minute);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + minute + " * * * ?"));
    }

    public void testSingleMinuteInvalid() throws Exception {
        try {
            new HourlySchedule(invalidMinute());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("invalid hourly minute"));
            assertThat(e.getMessage(), containsString("minute must be between 0 and 59 incl."));
        }
    }

    public void testMultipleMinutes() throws Exception {
        int[] minutes = validMinutes();
        String minutesStr = Strings.join(",", minutes);
        HourlySchedule schedule = new HourlySchedule(minutes);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + minutesStr + " * * * ?"));
    }

    public void testMultipleMinutesInvalid() throws Exception {
        int[] minutes = invalidMinutes();
        try {
            new HourlySchedule(minutes);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("invalid hourly minute"));
            assertThat(e.getMessage(), containsString("minute must be between 0 and 59 incl."));
        }
    }

    public void testParserEmpty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(0));
    }

    public void testParserSingleMinuteNumber() throws Exception {
        int minute = validMinute();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", minute)
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(minute));
    }

    public void testParserSingleMinuteNumberInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", invalidMinute())
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new HourlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hourly] schedule. invalid value for [minute]"));
        }
    }

    public void testParserSingleMinuteString() throws Exception {
        int minute = validMinute();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", String.valueOf(minute))
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(minute));
    }

    public void testParserSingleMinuteStringInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", String.valueOf(invalidMinute()))
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new HourlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hourly] schedule. invalid value for [minute]"));
        }
    }

    public void testParserMultipleMinutesNumbers() throws Exception {
        int[] minutes = validMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", minutes)
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(minutes.length));
        List<Integer> ints = Arrays.stream(schedule.minutes()).mapToObj(Integer::valueOf).collect(Collectors.toList());
        for (int i = 0; i < minutes.length; i++) {
            assertThat(ints, hasItem(minutes[i]));
        }
    }

    public void testParserMultipleMinutesNumbersInvalid() throws Exception {
        int[] minutes = invalidMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", minutes)
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new HourlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hourly] schedule. invalid value for [minute]"));
        }
    }

    public void testParserMultipleMinutesStrings() throws Exception {
        int[] minutes = validMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Arrays.stream(minutes).mapToObj(Integer::toString).collect(Collectors.toList()))
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(minutes.length));

        List<Integer> ints = Arrays.stream(schedule.minutes()).mapToObj(Integer::valueOf).collect(Collectors.toList());
        for (int i = 0; i < minutes.length; i++) {
            assertThat(ints, hasItem(minutes[i]));
        }
    }

    public void testParserMultipleMinutesStringsInvalid() throws Exception {
        int[] minutes = invalidMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Arrays.stream(minutes).mapToObj(Integer::toString).collect(Collectors.toList()))
                .endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new HourlySchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hourly] schedule. invalid value for [minute]"));
        }
    }
}
