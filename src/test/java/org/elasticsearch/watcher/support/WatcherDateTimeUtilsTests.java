/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentParser;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class WatcherDateTimeUtilsTests extends ElasticsearchTestCase {

    @Test
    public void testParseTimeValue_Numeric() throws Exception {
        TimeValue value = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", value.getMillis()).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(value.millis()));
    }

    @Test(expected = WatcherDateTimeUtils.ParseException.class)
    public void testParseTimeValue_Numeric_Negative() throws Exception {
        TimeValue value = new TimeValue(randomIntBetween(1, 100), randomFrom(MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", -1 * value.getMillis()).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        WatcherDateTimeUtils.parseTimeValue(parser, null, "test");
    }

    @Test
    public void testParseTimeValue_String() throws Exception {
        int value = randomIntBetween(2, 200);
        ImmutableMap<String, TimeValue> values = ImmutableMap.<String, TimeValue>builder()
                .put(value + "s", TimeValue.timeValueSeconds(value))
                .put(value + "m", TimeValue.timeValueMinutes(value))
                .put(value + "h", TimeValue.timeValueHours(value))
                .put(value + "", TimeValue.timeValueMillis(value))
                .build();

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", key).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(values.get(key).millis()));
    }

    @Test(expected = WatcherDateTimeUtils.ParseException.class)
    public void testParseTimeValue_String_Negative() throws Exception {
        int value = -1 * randomIntBetween(2, 200);
        ImmutableMap<String, TimeValue> values = ImmutableMap.<String, TimeValue>builder()
                .put(value + "s", TimeValue.timeValueSeconds(value))
                .put(value + "m", TimeValue.timeValueMinutes(value))
                .put(value + "h", TimeValue.timeValueHours(value))
                .put(value + "", TimeValue.timeValueMillis(value))
                .build();

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", key).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        WatcherDateTimeUtils.parseTimeValue(parser, null, "test");
    }

    @Test
    public void testParseTimeValue_Null() throws Exception {
        XContentParser parser = xContentParser(jsonBuilder().startObject().nullField("value").endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null, "test");
        assertThat(parsed, nullValue());
    }

    @Test
    public void testParseTimeValue_Null_DefaultValue() throws Exception {
        XContentParser parser = xContentParser(jsonBuilder().startObject().nullField("value").endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue defaultValue = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, defaultValue, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed, sameInstance(defaultValue));
    }
}
