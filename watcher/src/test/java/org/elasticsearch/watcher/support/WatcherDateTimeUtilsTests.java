/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;


import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentParser;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class WatcherDateTimeUtilsTests extends ElasticsearchTestCase {

    @Test(expected = ElasticsearchParseException.class)
    public void testParseTimeValue_Numeric() throws Exception {
        TimeValue value = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));
        long millis = value.getMillis();
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        if (randomBoolean() || millis == 0) { // 0 is special - no unit required
            xContentBuilder.field("value", millis);
        } else {
            xContentBuilder.field("value", Long.toString(millis));
        }
        XContentParser parser = xContentParser(xContentBuilder.endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(value.millis()));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParseTimeValue_Numeric_Negative() throws Exception {
        TimeValue value = new TimeValue(randomIntBetween(1, 100), randomFrom(MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", -1 * value.getMillis()).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        WatcherDateTimeUtils.parseTimeValue(parser, "test");
    }

    @Test
    public void testParseTimeValue_String() throws Exception {
        int value = randomIntBetween(2, 200);
        ImmutableMap<String, TimeValue> values = ImmutableMap.<String, TimeValue>builder()
                .put(value + "s", TimeValue.timeValueSeconds(value))
                .put(value + "m", TimeValue.timeValueMinutes(value))
                .put(value + "h", TimeValue.timeValueHours(value))
                .build();

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", key).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(values.get(key).millis()));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParseTimeValue_String_Negative() throws Exception {
        int value = -1 * randomIntBetween(2, 200);
        ImmutableMap<String, TimeValue> values = ImmutableMap.<String, TimeValue>builder()
                .put(value + "s", TimeValue.timeValueSeconds(value))
                .put(value + "m", TimeValue.timeValueMinutes(value))
                .put(value + "h", TimeValue.timeValueHours(value))
                .build();

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", key).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        WatcherDateTimeUtils.parseTimeValue(parser, "test");
    }

    public void testParseTimeValue_Null() throws Exception {
        XContentParser parser = xContentParser(jsonBuilder().startObject().nullField("value").endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, "test");
        assertThat(parsed, nullValue());
    }
}
