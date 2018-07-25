/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class IntervalScheduleTests extends ESTestCase {
    public void testParseNumber() throws Exception {
        long value = randomIntBetween(0, Integer.MAX_VALUE);
        XContentBuilder builder = jsonBuilder().value(value);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        IntervalSchedule schedule = new IntervalSchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.interval().seconds(), is(value));
    }

    public void testParseNegativeNumber() throws Exception {
        long value = randomIntBetween(Integer.MIN_VALUE, 0);
        XContentBuilder builder = jsonBuilder().value(value);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new IntervalSchedule.Parser().parse(parser);
            fail("exception expected, because interval is negative");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getCause().getMessage(), containsString("interval can't be lower than 1000 ms, but"));
        }
    }

    public void testParseString() throws Exception {
        IntervalSchedule.Interval value = randomTimeInterval();
        XContentBuilder builder = jsonBuilder().value(value);
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        IntervalSchedule schedule = new IntervalSchedule.Parser().parse(parser);
        assertThat(schedule, notNullValue());
        assertThat(schedule.interval(), is(value));
    }

    public void testParseInvalidString() throws Exception {
        XContentBuilder builder = jsonBuilder().value("43S");
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new IntervalSchedule.Parser().parse(parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("unrecognized interval format [43S]"));
        }
    }

    public void testParseInvalidObject() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken(); // advancing to the start object
        try {
            new IntervalSchedule.Parser().parse(parser);
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(),
                    containsString("expected either a numeric value (millis) or a string value representing time value"));
            assertThat(e.getMessage(), containsString("found [START_OBJECT]"));
        }
    }

    private static IntervalSchedule.Interval randomTimeInterval() {
        int randomSize = randomIntBetween(0, IntervalSchedule.Interval.Unit.values().length - 1);
        IntervalSchedule.Interval.Unit unit = IntervalSchedule.Interval.Unit.values()[randomSize];
        return new IntervalSchedule.Interval(randomIntBetween(1, 100), unit);
    }
}
