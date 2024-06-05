/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;

public class ConnectorUtilsTests extends ESTestCase {

    public void testParseInstantConnectorFrameworkFormat() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "\"2023-01-16T10:00:00.123+00:00\"");
        parser.nextToken();
        Instant instant = ConnectorUtils.parseInstant(parser, "my_time_field");
        assertNotNull(instant);
        assertEquals(1673863200123L, instant.toEpochMilli());
    }

    public void testParseInstantStandardJavaFormat() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "\"2023-01-16T10:00:00.123000000Z\"");
        parser.nextToken();
        Instant instant = ConnectorUtils.parseInstant(parser, "my_time_field");
        assertNotNull(instant);
        assertEquals(1673863200123L, instant.toEpochMilli());
    }

    public void testParseInstantStandardJavaFormatWithNanosecondPrecision() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "\"2023-01-16T10:00:00.123456789Z\"");
        parser.nextToken();
        Instant instant = ConnectorUtils.parseInstant(parser, "my_time_field");
        assertNotNull(instant);
        assertEquals(123456789L, instant.getNano());
        assertEquals(1673863200L, instant.getEpochSecond());
    }

    public void testParseNullableInstant() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray("null"));
        parser.nextToken();
        Instant instant = ConnectorUtils.parseNullableInstant(parser, "my_time_field");
        assertNull(instant);
    }

    public void testParseNullableInstantWithValue() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "\"2023-01-16T10:00:00.123+00:00\"");
        parser.nextToken();
        Instant instant = ConnectorUtils.parseNullableInstant(parser, "my_time_field");
        assertNotNull(instant);
        assertEquals(1673863200123L, instant.toEpochMilli());
    }

}
