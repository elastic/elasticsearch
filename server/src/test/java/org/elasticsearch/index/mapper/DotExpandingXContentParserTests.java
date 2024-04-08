/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DotExpandingXContentParserTests extends ESTestCase {

    private void assertXContentMatches(String dotsExpanded, String withDots) throws IOException {
        final ContentPath contentPath = new ContentPath();
        try (
            XContentParser inputParser = createParser(JsonXContent.jsonXContent, withDots);
            XContentParser expandedParser = DotExpandingXContentParser.expandDots(inputParser, contentPath)
        ) {
            expandedParser.allowDuplicateKeys(true);

            XContentBuilder actualOutput = XContentBuilder.builder(JsonXContent.jsonXContent).copyCurrentStructure(expandedParser);
            assertEquals(dotsExpanded, Strings.toString(actualOutput));

            try (XContentParser expectedParser = createParser(JsonXContent.jsonXContent, dotsExpanded)) {
                expectedParser.allowDuplicateKeys(true);
                try (
                    var p = createParser(JsonXContent.jsonXContent, withDots);
                    XContentParser actualParser = DotExpandingXContentParser.expandDots(p, contentPath)
                ) {
                    XContentParser.Token currentToken;
                    while ((currentToken = actualParser.nextToken()) != null) {
                        assertEquals(currentToken, expectedParser.nextToken());
                        assertEquals(expectedParser.currentToken(), actualParser.currentToken());
                        assertEquals(actualParser.currentToken().name(), expectedParser.currentName(), actualParser.currentName());
                    }
                    assertNull(expectedParser.nextToken());
                }
            }
        }
    }

    public void testEmbeddedObject() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":{"field":"value"}}},"nodots":"value2"}\
            """, """
            {"test.with.dots":{"field":"value"},"nodots":"value2"}\
            """);
    }

    public void testEmbeddedObjects() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":{"obj":{"field":"value","array":["value",{"field":"value"}]}}}},"nodots":"value2"}\
            """, """
            {"test.with.dots":{"obj":{"field":"value","array":["value",{"field":"value"}]}},"nodots":"value2"}\
            """);
    }

    public void testEmbeddedArrayOfValues() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":["field","value"]}},"nodots":"value2"}\
            """, """
            {"test.with.dots":["field","value"],"nodots":"value2"}\
            """);

    }

    public void testEmbeddedArrayOfObjects() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":[{"field":"value"},{"field":"value"}]}},"nodots":"value2"}\
            """, """
            {"test.with.dots":[{"field":"value"},{"field":"value"}],"nodots":"value2"}\
            """);

    }

    public void testEmbeddedArrayMixedContent() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":["value",{"field":"value"}]}},"nodots":"value2"}\
            """, """
            {"test.with.dots":["value",{"field":"value"}],"nodots":"value2"}\
            """);

    }

    public void testEmbeddedValue() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":"value"}},"nodots":"value2"}\
            """, """
            {"test.with.dots":"value","nodots":"value2"}\
            """);

    }

    public void testTrailingDotsAreStripped() throws IOException {

        assertXContentMatches("""
            {"test":{"with":{"dots":"value"}},"nodots":"value"}""", """
            {"test.":{"with.":{"dots":"value"}},"nodots":"value"}""");

    }

    public void testDuplicateKeys() throws IOException {
        assertXContentMatches("""
            {"test":{"with":{"dots1":"value1"}},"test":{"with":{"dots2":"value2"}}}""", """
            { "test.with.dots1" : "value1",
              "test.with.dots2" : "value2"}""");
    }

    public void testDotsCollapsingFlatPaths() throws IOException {
        ContentPath contentPath = new ContentPath();
        XContentParser parser = DotExpandingXContentParser.expandDots(createParser(JsonXContent.jsonXContent, """
            {"metrics.service.time": 10, "metrics.service.time.max": 500, "metrics.foo": "value"}"""), contentPath);
        parser.nextToken();
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(true);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("time", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals("time", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(false);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(true);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("time.max", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals("time.max", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(false);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.currentName());
        assertNull(parser.nextToken());
    }

    public void testDotsCollapsingStructuredPath() throws IOException {
        ContentPath contentPath = new ContentPath();
        XContentParser parser = DotExpandingXContentParser.expandDots(createParser(JsonXContent.jsonXContent, """
            {
              "metrics" : {
                "service" : {
                  "time" : 10,
                  "time.max" : 500
                },
                "foo" : "value"
              }
            }"""), contentPath);
        parser.nextToken();
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(true);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("time", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals("time", parser.currentName());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("time.max", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals("time.max", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("service", parser.currentName());
        contentPath.setWithinLeafObject(false);
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("foo", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals("metrics", parser.currentName());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.currentName());
        assertNull(parser.nextToken());
    }

    public void testSkipChildren() throws IOException {
        XContentParser parser = DotExpandingXContentParser.expandDots(createParser(JsonXContent.jsonXContent, """
            { "test.with.dots" : "value", "nodots" : "value2" }"""), new ContentPath());
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

    public void testSkipChildrenWithinInnerObject() throws IOException {
        XContentParser parser = DotExpandingXContentParser.expandDots(createParser(JsonXContent.jsonXContent, """
            { "test.with.dots" : {"obj" : {"field":"value"}}, "nodots" : "value2" }"""), new ContentPath());

        parser.nextToken();     // start object
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("test", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("with", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("dots", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("obj", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        parser.skipChildren();
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
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

    public void testGetTokenLocation() throws IOException {
        String jsonInput = """
            {"first.dot":{"second.dot":"value",
            "value":null}}\
            """;
        XContentParser expectedParser = createParser(JsonXContent.jsonXContent, jsonInput);
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, jsonInput),
            new ContentPath()
        );

        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, expectedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.FIELD_NAME, expectedParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals("first", dotExpandedParser.currentName());
        assertEquals("first.dot", expectedParser.currentName());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals("dot", dotExpandedParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, expectedParser.nextToken());
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.FIELD_NAME, expectedParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals("second", dotExpandedParser.currentName());
        assertEquals("second.dot", expectedParser.currentName());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals("dot", dotExpandedParser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, expectedParser.nextToken());
        assertEquals(XContentParser.Token.VALUE_STRING, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.END_OBJECT, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.FIELD_NAME, expectedParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals("value", dotExpandedParser.currentName());
        assertEquals("value", expectedParser.currentName());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.VALUE_NULL, expectedParser.nextToken());
        assertEquals(XContentParser.Token.VALUE_NULL, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.END_OBJECT, dotExpandedParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, expectedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.END_OBJECT, dotExpandedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertEquals(XContentParser.Token.END_OBJECT, dotExpandedParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, expectedParser.nextToken());
        assertEquals(expectedParser.getTokenLocation(), dotExpandedParser.getTokenLocation());
        assertNull(dotExpandedParser.nextToken());
        assertNull(expectedParser.nextToken());
    }

    public void testParseMapUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, dotExpandedParser::map);
    }

    public void testParseMapOrderedUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, dotExpandedParser::mapOrdered);
    }

    public void testParseMapStringsUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, dotExpandedParser::mapStrings);
    }

    public void testParseMapSupplierUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, () -> dotExpandedParser.map(HashMap::new, XContentParser::text));
    }

    public void testParseMap() throws Exception {
        String jsonInput = """
            {"params":{"one":"one",
            "two":"two"}}\
            """;

        ContentPath contentPath = new ContentPath();
        contentPath.setWithinLeafObject(true);
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, jsonInput),
            contentPath
        );
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals("params", dotExpandedParser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        Map<String, Object> map = dotExpandedParser.map();
        assertEquals(2, map.size());
        assertEquals("one", map.get("one"));
        assertEquals("two", map.get("two"));
    }

    public void testParseListUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, dotExpandedParser::list);
    }

    public void testParseListOrderedUOE() throws Exception {
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, ""),
            new ContentPath()
        );
        expectThrows(UnsupportedOperationException.class, dotExpandedParser::listOrderedMap);
    }

    public void testParseList() throws Exception {
        String jsonInput = """
            {"params":["one","two"]}\
            """;

        ContentPath contentPath = new ContentPath();
        contentPath.setWithinLeafObject(true);
        XContentParser dotExpandedParser = DotExpandingXContentParser.expandDots(
            createParser(JsonXContent.jsonXContent, jsonInput),
            contentPath
        );
        assertEquals(XContentParser.Token.START_OBJECT, dotExpandedParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, dotExpandedParser.nextToken());
        assertEquals("params", dotExpandedParser.currentName());
        List<Object> list = dotExpandedParser.list();
        assertEquals(2, list.size());
        assertEquals("one", list.get(0));
        assertEquals("two", list.get(1));
    }
}
