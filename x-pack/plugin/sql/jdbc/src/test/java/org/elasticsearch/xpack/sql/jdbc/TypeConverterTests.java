/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.Matchers.instanceOf;


public class TypeConverterTests extends ESTestCase {

    public void testFloatAsNative() throws Exception {
        assertThat(convertAsNative(42.0f, EsType.FLOAT), instanceOf(Float.class));
        assertThat(convertAsNative(42.0, EsType.FLOAT), instanceOf(Float.class));
        assertEquals(42.0f, (float) convertAsNative(42.0, EsType.FLOAT), 0.001f);
        assertEquals(Float.NaN, convertAsNative(Float.NaN, EsType.FLOAT));
        assertEquals(Float.NEGATIVE_INFINITY, convertAsNative(Float.NEGATIVE_INFINITY, EsType.FLOAT));
        assertEquals(Float.POSITIVE_INFINITY, convertAsNative(Float.POSITIVE_INFINITY, EsType.FLOAT));
    }

    public void testDoubleAsNative() throws Exception {
        EsType type = randomFrom(EsType.HALF_FLOAT, EsType.SCALED_FLOAT, EsType.DOUBLE);
        assertThat(convertAsNative(42.0, type), instanceOf(Double.class));
        assertEquals(42.0f, (double) convertAsNative(42.0, type), 0.001f);
        assertEquals(Double.NaN, convertAsNative(Double.NaN, type));
        assertEquals(Double.NEGATIVE_INFINITY, convertAsNative(Double.NEGATIVE_INFINITY, type));
        assertEquals(Double.POSITIVE_INFINITY, convertAsNative(Double.POSITIVE_INFINITY, type));
    }

    public void testTimestampAsNative() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(Clock.tick(Clock.system(ZoneId.of("Z")), Duration.ofMillis(1)));
        assertThat(convertAsNative(now, EsType.DATETIME), instanceOf(Timestamp.class));
        assertEquals(now.toInstant().toEpochMilli(), ((Timestamp) convertAsNative(now, EsType.DATETIME)).getTime());
    }

    private Object convertAsNative(Object value, EsType type) throws Exception {
        // Simulate sending over XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("value");
        builder.value(value);
        builder.endObject();
        builder.close();
        Object copy = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2().get("value");
        return TypeConverter.convert(copy, type, type.toString());
    }
}
