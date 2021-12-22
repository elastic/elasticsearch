/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class DotExpandingXContentParserTests extends ESTestCase {

    private void assertXContentMatches(String expected, String actual) throws IOException {
        XContentParser inputParser = createParser(JsonXContent.jsonXContent, actual);
        XContentParser expandedParser = DotExpandingXContentParser.expandDots(inputParser);

        XContentBuilder actualOutput = XContentBuilder.builder(JsonXContent.jsonXContent).copyCurrentStructure(expandedParser);
        assertEquals(expected, Strings.toString(actualOutput));
    }

    public void testEmbeddedObject() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":{"field":"value"}}},"nodots":"value2"}\
            """, """
            {"test.with.dots":{"field":"value"},"nodots":"value2"}\
            """);
    }

    public void testEmbeddedArray() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":["field","value"]}},"nodots":"value2"}\
            """, """
            {"test.with.dots":["field","value"],"nodots":"value2"}\
            """);

    }

    public void testEmbeddedValue() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":"value"}},"nodots":"value2"}\
            """, """
            {"test.with.dots":"value","nodots":"value2"}\
            """);

    }

    public void testSkipChildren() throws IOException {
        XContentParser parser = DotExpandingXContentParser.expandDots(createParser(JsonXContent.jsonXContent, """
            { "test.with.dots" : "value", "nodots" : "value2" }"""));

        parser.nextToken();     // start object
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("test", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("with", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        parser.skipChildren();
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("nodots", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value2", parser.text());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testNestedExpansions() throws IOException {
        assertXContentMatches("""
            {"first":{"dot":{"second":{"dot":"value"},"third":"value"}},"nodots":"value"}\
            """, """
            {"first.dot":{"second.dot":"value","third":"value"},"nodots":"value"}\
            """);
    }
}
