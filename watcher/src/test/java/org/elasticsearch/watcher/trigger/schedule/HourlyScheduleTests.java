/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import com.google.common.primitives.Ints;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class HourlyScheduleTests extends ScheduleTestCase {

    @Test
    public void test_Default() throws Exception {
        HourlySchedule schedule = new HourlySchedule();
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 0 * * * ?"));
    }

    @Test
    public void test_SingleMinute() throws Exception {
        int minute = validMinute();
        HourlySchedule schedule = new HourlySchedule(minute);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + minute + " * * * ?"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_SingleMinute_Invalid() throws Exception {
        new HourlySchedule(invalidMinute());
    }

    @Test
    public void test_MultipleMinutes() throws Exception {
        int[] minutes = validMinutes();
        String minutesStr = Ints.join(",", minutes);
        HourlySchedule schedule = new HourlySchedule(minutes);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(1));
        assertThat(crons, arrayContaining("0 " + minutesStr + " * * * ?"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_MultipleMinutes_Invalid() throws Exception {
        int[] minutes = invalidMinutes();
        new HourlySchedule(minutes);
    }

    @Test
    public void testParser_Empty() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(0));
    }

    @Test
    public void testParser_SingleMinute_Number() throws Exception {
        int minute = validMinute();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", minute)
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(minute));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParser_SingleMinute_Number_Invalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", invalidMinute())
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new HourlySchedule.Parser().parse(parser);
    }

    @Test
    public void testParser_SingleMinute_String() throws Exception {
        int minute = validMinute();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", String.valueOf(minute))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(1));
        assertThat(schedule.minutes()[0], is(minute));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParser_SingleMinute_String_Invalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", String.valueOf(invalidMinute()))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new HourlySchedule.Parser().parse(parser);
    }

    @Test
    public void testParser_MultipleMinutes_Numbers() throws Exception {
        int[] minutes = validMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Ints.asList(minutes))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(minutes.length));
        for (int i = 0; i < minutes.length; i++) {
            assertThat(Ints.contains(schedule.minutes(), minutes[i]), is(true));
        }
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParser_MultipleMinutes_Numbers_Invalid() throws Exception {
        int[] minutes = invalidMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Ints.asList(minutes))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new HourlySchedule.Parser().parse(parser);
    }

    @Test
    public void testParser_MultipleMinutes_Strings() throws Exception {
        int[] minutes = validMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Ints.asList(minutes).stream().map(p -> p.toString()).collect(Collectors.toList()))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        HourlySchedule schedule = new HourlySchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.minutes().length, is(minutes.length));
        for (int i = 0; i < minutes.length; i++) {
            assertThat(Ints.contains(schedule.minutes(), minutes[i]), is(true));
        }
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParser_MultipleMinutes_Strings_Invalid() throws Exception {
        int[] minutes = invalidMinutes();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("minute", Ints.asList(minutes).stream().map(p -> p.toString()).collect(Collectors.toList()))
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new HourlySchedule.Parser().parse(parser);
    }

}
