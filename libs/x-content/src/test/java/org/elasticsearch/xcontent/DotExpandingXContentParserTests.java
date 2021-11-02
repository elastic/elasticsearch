/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class DotExpandingXContentParserTests extends ESTestCase {

    public void testEmbeddedObject() throws IOException {

        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"test.with.dots\" : { \"field\" : \"value\" }, \"nodots\" : \"value2\" }"
        );

        parser.nextToken();     // start object
        parser.nextToken();     // test.with.dots fieldname

        XContentParser testParser = DotExpandingXContentParser.expandDots(parser);
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.currentToken());
        assertEquals("test", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("with", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("dots", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("field", testParser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, testParser.nextToken());
        assertEquals("value", testParser.text());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());

        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("nodots", testParser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, testParser.nextToken());
        assertEquals("value2", testParser.text());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertNull(testParser.nextToken());
    }

    public void testEmbeddedArray() throws IOException {

        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"test.with.dots\" : [ \"field\", \"value\" ], \"nodots\" : \"value2\" }"
        );

        parser.nextToken();     // start object
        parser.nextToken();     // test.with.dots fieldname

        XContentParser testParser = DotExpandingXContentParser.expandDots(parser);
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.currentToken());
        assertEquals("test", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("with", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("dots", testParser.currentName());
        assertEquals(XContentParser.Token.START_ARRAY, testParser.nextToken());
        assertEquals(XContentParser.Token.START_ARRAY, testParser.currentToken());
        assertEquals(XContentParser.Token.VALUE_STRING, testParser.nextToken());
        assertEquals("field", testParser.text());
        assertEquals(XContentParser.Token.VALUE_STRING, testParser.nextToken());
        assertEquals("value", testParser.text());
        assertEquals(XContentParser.Token.END_ARRAY, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("nodots", testParser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value2", parser.text());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testEmbeddedValue() throws IOException {

        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test.with.dots\" : \"value\", \"nodots\" : \"value2\" }");

        parser.nextToken();     // start object
        parser.nextToken();     // test.with.dots fieldname

        XContentParser testParser = DotExpandingXContentParser.expandDots(parser);
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.currentToken());
        assertEquals("test", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("with", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("dots", testParser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, testParser.nextToken());
        assertEquals("value", testParser.text());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("nodots", testParser.currentName());
        assertEquals("nodots", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value2", testParser.text());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertNull(testParser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testSkipChildren() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test.with.dots\" : \"value\", \"nodots\" : \"value2\" }");

        parser.nextToken();     // start object
        parser.nextToken();     // test.with.dots fieldname

        XContentParser testParser = DotExpandingXContentParser.expandDots(parser);
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.currentToken());
        assertEquals("test", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, testParser.nextToken());
        assertEquals("with", testParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, testParser.currentToken());
        testParser.skipChildren();
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("nodots", testParser.currentName());
        assertEquals("nodots", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value2", testParser.text());
        assertEquals(XContentParser.Token.END_OBJECT, testParser.nextToken());
        assertNull(testParser.nextToken());
        assertNull(parser.nextToken());
    }
}
