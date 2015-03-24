/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.scheduler.schedule;

import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.scheduler.schedule.IntervalSchedule.Interval.Unit;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class IntervalScheduleTests extends ElasticsearchTestCase {

    @Test
    public void testParse_Number() throws Exception {
        long value = (long) randomInt();
        XContentBuilder builder = jsonBuilder().value(value);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        IntervalSchedule schedule = new IntervalSchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.interval().seconds(), is(value));
    }

    @Test
    public void testParse_String() throws Exception {
        IntervalSchedule.Interval value = randomTimeValue();
        XContentBuilder builder = jsonBuilder().value(value);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        IntervalSchedule schedule = new IntervalSchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.interval(), is(value));
    }

    @Test(expected = WatcherSettingsException.class)
    public void testParse_Invalid_String() throws Exception {
        XContentBuilder builder = jsonBuilder().value("43S");
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new IntervalSchedule.Parser().parse(parser);
    }

    @Test(expected = WatcherSettingsException.class)
    public void testParse_Invalid_Object() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken(); // advancing to the start object
        new IntervalSchedule.Parser().parse(parser);
    }

    private static IntervalSchedule.Interval randomTimeValue() {
        Unit unit = Unit.values()[randomIntBetween(0, Unit.values().length - 1)];
        return new IntervalSchedule.Interval(randomIntBetween(1, 100), unit);
    }
}
