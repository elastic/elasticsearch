/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.support;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class DateTimeUtilsTests extends ESTestCase {
    public void testParseTimeValueNumeric() throws Exception {
        TimeValue value = new TimeValue(randomInt(100), randomFrom(TimeUnit.values()));
        long millis = value.getMillis();
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        if (randomBoolean() || millis == 0) { // 0 is special - no unit required
            xContentBuilder.field("value", millis);
        } else {
            xContentBuilder.field("value", Long.toString(millis));
        }
        final XContentBuilder builder = xContentBuilder.endObject();
        XContentParser parser = builder.contentType().xContent().createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        try {
            DateTimeUtils.parseTimeValue(parser, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), either(is("failed to parse time unit"))
                    .or(is("could not parse time value. expected either a string or a null value but found [VALUE_NUMBER] instead")));
        }
    }

    public void testParseTimeValueNumericNegative() throws Exception {
        TimeValue value = new TimeValue(randomIntBetween(1, 100), randomFrom(MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS));

        final XContentBuilder builder = jsonBuilder().startObject().field("value", -1 * value.getMillis()).endObject();
        XContentParser parser = builder.contentType().xContent().createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        try {
            DateTimeUtils.parseTimeValue(parser, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(),
                    is("could not parse time value. expected either a string or a null value but found [VALUE_NUMBER] instead"));
        }
    }

    public void testParseTimeValueString() throws Exception {
        int value = randomIntBetween(2, 200);
        Map<String, TimeValue> values = new HashMap<>();
        values.put(value + "s", TimeValue.timeValueSeconds(value));
        values.put(value + "m", TimeValue.timeValueMinutes(value));
        values.put(value + "h", TimeValue.timeValueHours(value));

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        final XContentBuilder builder = jsonBuilder().startObject().field("value", key).endObject();
        XContentParser parser = builder.contentType().xContent().createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = DateTimeUtils.parseTimeValue(parser, "test");
        assertThat(parsed, notNullValue());
        assertThat(parsed.millis(), is(values.get(key).millis()));
    }

    public void testParseTimeValueStringNegative() throws Exception {
        int value = -1 * randomIntBetween(2, 200);
        Map<String, TimeValue> values = new HashMap<>();
        values.put(value + "s", TimeValue.timeValueSeconds(value));
        values.put(value + "m", TimeValue.timeValueMinutes(value));
        values.put(value + "h", TimeValue.timeValueHours(value));

        String key = randomFrom(values.keySet().toArray(new String[values.size()]));

        final XContentBuilder builder = jsonBuilder().startObject().field("value", key).endObject();
        XContentParser parser = builder.contentType().xContent().createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        try {
            DateTimeUtils.parseTimeValue(parser, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("failed to parse time unit"));
        }
    }

    public void testParseTimeValueNull() throws Exception {
        final XContentBuilder builder = jsonBuilder().startObject().nullField("value").endObject();
        XContentParser parser = builder.contentType().xContent().createParser(builder.bytes());
        parser.nextToken(); // start object
        parser.nextToken(); // field name
        parser.nextToken(); // value

        TimeValue parsed = DateTimeUtils.parseTimeValue(parser, "test");
        assertThat(parsed, nullValue());
    }
}
