/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

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

    @Test @Repeat(iterations = 20)
    public void testParseTimeValue_Numeric() throws Exception {
        TimeValue value = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", value.getMillis()).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null);
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(value.millis()));
    }

    @Test @Repeat(iterations = 10)
    public void testParseTimeValue_String() throws Exception {
        ImmutableMap<String, TimeValue> values = ImmutableMap.<String, TimeValue>builder()
                .put("5s", TimeValue.timeValueSeconds(5))
                .put("5m", TimeValue.timeValueMinutes(5))
                .put("5h", TimeValue.timeValueHours(5))
                .put("5", TimeValue.timeValueMillis(5))
                .build();

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        XContentParser parser = xContentParser(jsonBuilder().startObject().field("value", key).endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null);
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(values.get(key).millis()));
    }

    @Test
    public void testParseTimeValue_Null() throws Exception {
        XContentParser parser = xContentParser(jsonBuilder().startObject().nullField("value").endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, null);
        assertThat(parsed, nullValue());
    }

    @Test
    public void testParseTimeValue_Null_DefaultValue() throws Exception {
        XContentParser parser = xContentParser(jsonBuilder().startObject().nullField("value").endObject());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue defaultValue = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));

        TimeValue parsed = WatcherDateTimeUtils.parseTimeValue(parser, defaultValue);
        assertThat(parsed, notNullValue());
        assertThat(parsed, sameInstance(defaultValue));
    }
}
