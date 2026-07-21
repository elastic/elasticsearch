/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class XContentUtilsTests extends ESTestCase {

    public void testMoveToFirstToken() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            assertNull(parser.currentToken());

            XContentUtils.moveToFirstToken(parser);

            assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        }
    }

    public void testMoveToFirstToken_DoesNotMoveIfAlreadyAtAToken() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            // position at a valid token
            parser.nextToken();
            assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());

            XContentUtils.moveToFirstToken(parser);

            // still at the beginning of the object
            assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());
        }
    }

    public void testPositionParserAtTokenAfterField() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentUtils.positionParserAtTokenAfterField(parser, "key", "some error");

            assertEquals("value", parser.text());
        }
    }

    public void testPositionParserAtTokenAfterField_ThrowsIfFieldIsMissing() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;
        var errorFormat = "Error: %s";
        var missingField = "missing field";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            var exception = expectThrows(
                IllegalStateException.class,
                () -> XContentUtils.positionParserAtTokenAfterField(parser, missingField, errorFormat)
            );

            assertEquals(String.format(Locale.ROOT, errorFormat, missingField), exception.getMessage());
        }
    }

    public void testPositionParserAtTokenAfterField_ThrowsWithMalformedJSON() throws IOException {
        var json = """
            {
                "key": "value",
                "foo": "bar"
            """;
        var errorFormat = "Error: %s";
        var missingField = "missing field";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            var exception = expectThrows(
                XContentEOFException.class,
                () -> XContentUtils.positionParserAtTokenAfterField(parser, missingField, errorFormat)
            );
            assertThat(exception.getMessage(), containsString("[4:1] Unexpected end of file"));
        }
    }

    public void testPositionParserAtTokenAfterField_ConsumesUntilEnd() throws IOException {
        var json = """
            {
              "key": {
                "foo": "bar"
              },
              "target": "value"
            }
            """;

        var errorFormat = "Error: %s";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentUtils.positionParserAtTokenAfterField(parser, "target", errorFormat);
            assertEquals("value", parser.text());
        }
    }

    public void testPositionParserAtTokenAfterFieldCurrentObj() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            XContentUtils.positionParserAtTokenAfterFieldCurrentFlatObj(parser, "key", "some error");

            assertEquals("value", parser.text());
        }
    }

    public void testPositionParserAtTokenAfterFieldCurrentObj_ThrowsIfFieldIsMissing() throws IOException {
        var json = """
            {
                "key": "value"
            }
            """;
        var errorFormat = "Error: %s";
        var missingField = "missing field";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            var exception = expectThrows(
                IllegalStateException.class,
                () -> XContentUtils.positionParserAtTokenAfterFieldCurrentFlatObj(parser, missingField, errorFormat)
            );

            assertEquals(String.format(Locale.ROOT, errorFormat, missingField), exception.getMessage());
        }
    }

    public void testPositionParserAtTokenAfterFieldCurrentObj_DoesNotFindNested() throws IOException {
        var json = """
            {
                "nested": {
                    "key": "value"
                }
            }
            """;
        var errorFormat = "Error: %s";
        var missingField = "missing field";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            var exception = expectThrows(
                IllegalStateException.class,
                () -> XContentUtils.positionParserAtTokenAfterFieldCurrentFlatObj(parser, missingField, errorFormat)
            );

            assertEquals(String.format(Locale.ROOT, errorFormat, missingField), exception.getMessage());
        }
    }

    public void testConsumeUntilObjectEnd() throws IOException {
        var json = """
            {
                "key": "value",
                "foo": true,
                "bar": 0.1
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            parser.nextToken();
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken()); // fully parsed
        }
    }

    public void testConsumeUntilObjectEnd_SkipArray() throws IOException {
        var json = """
            {
                "key": "value",
                "skip_array": [1.0, 2.0, 3.0]
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    public void testConsumeUntilObjectEnd_SkipNestedObject() throws IOException {
        var json = """
            {
                "key": "value",
                "skip_obj": {
                  "foo": "bar"
                }
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken()); // fully parsed
        }
    }

    public void testConsumeUntilObjectEnd_InArray() throws IOException {
        var json = """
            [
                {
                    "key": "value",
                    "skip_obj": {
                      "foo": "bar"
                    }
                },
                {
                    "key": "value",
                    "skip_array": [1.0, 2.0, 3.0]
                },
                {
                    "key": "value",
                    "skip_field1": "f1",
                    "skip_field2": "f2"
                }
            ]
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

            // Parser now inside object 1
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("key", parser.currentName());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());

            // Start of object 2
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());

            // Start of object 3
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("skip_field1", parser.currentName());
            XContentUtils.consumeUntilObjectEnd(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());

            assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
            assertNull(parser.nextToken()); // fully parsed
        }
    }

    public void testParseFloat_SingleFloatValue() throws IOException {
        var json = """
             {
               "key": 1.23
              }
            """;
        var errorFormat = "Error: %s";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentUtils.positionParserAtTokenAfterField(parser, "key", errorFormat);
            Float value = XContentUtils.parseFloat(parser);

            assertThat(value, equalTo(1.23F));
        }
    }

    public void testParseFloat_SingleIntValue() throws IOException {
        var json = """
             {
               "key": 1
             }
            """;
        var errorFormat = "Error: %s";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentUtils.positionParserAtTokenAfterField(parser, "key", errorFormat);
            Float value = XContentUtils.parseFloat(parser);

            assertThat(value, equalTo(1.0F));
        }
    }

    public void testParseFloat_ThrowsIfNotANumber() throws IOException {
        var json = """
             {
               "key": "value"
             }
            """;
        var errorFormat = "Error: %s";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentUtils.positionParserAtTokenAfterField(parser, "key", errorFormat);
            expectThrows(ParsingException.class, () -> XContentUtils.parseFloat(parser));
        }
    }

    public void testParseObjects_SingleObject() throws IOException {
        var data = """
            {"key":"value"}
            """;
        var results = XContentUtils.parseObjects(XContentParserConfiguration.EMPTY, data, p -> Stream.of(p.map().get("key"))).toList();
        assertThat(results.size(), is(1));
        assertThat(results.get(0), is("value"));
    }

    public void testParseObjects_TwoObjectsJoinedByNewline() throws IOException {
        var data = """
            {"key":"first"}
            {"key":"second"}
            {"key":"third",
            "key2":"fourth"}
            """;
        var results = XContentUtils.parseObjects(
            XContentParserConfiguration.EMPTY,
            data,
            p -> Stream.of(p.map().values()).flatMap(Collection::stream)
        ).toList();
        assertThat(results.size(), is(4));
        assertThat(results, containsInAnyOrder("first", "second", "third", "fourth"));
    }

    public void testParseObjects_WithSubParser_EarlyStopBothObjectsConsumed() throws IOException {
        // Verifies that when the objectParser wraps in XContentSubParser and stops early (does not
        // read to END_OBJECT), XContentSubParser.close() drains the remainder so the outer loop
        // correctly advances to the next root object.
        var data = """
            {"key":"first","other":"ignored"}
            {"key":"second","other":"ignored"}
            """;
        var results = XContentUtils.parseObjects(XContentParserConfiguration.EMPTY, data, p -> {
            try (var sub = new XContentSubParser(p)) {
                XContentUtils.positionParserAtTokenAfterField(sub, "key", "Error: %s");
                // Eagerly capture the text before the subparser closes and drains.
                return Stream.of(sub.text());
            }
        }).toList();
        assertThat(results.size(), is(2));
        assertThat(results.get(0), is("first"));
        assertThat(results.get(1), is("second"));
    }

    public void testParseObjects_EmptyData_ReturnsEmptyStream() throws IOException {
        var results = XContentUtils.parseObjects(XContentParserConfiguration.EMPTY, "", p -> Stream.of("unreachable")).toList();
        assertThat(results.size(), is(0));
    }

    public void testParseObjects_NonObjectRoot_Throws() {
        expectThrows(
            ParsingException.class,
            () -> XContentUtils.parseObjects(XContentParserConfiguration.EMPTY, "[1,2,3]", p -> Stream.empty())
        );
    }

    public void testParseObjects_FlatMapsAcrossObjects() throws IOException {
        var data = """
            {"emit":false}
            {"emit":true}
            {"emit":false}
            """;
        var results = XContentUtils.parseObjects(XContentParserConfiguration.EMPTY, data, p -> {
            var map = p.map();
            return Boolean.TRUE.equals(map.get("emit")) ? Stream.of("emitted") : Stream.empty();
        }).toList();
        assertThat(results.size(), is(1));
        assertThat(results.get(0), is("emitted"));
    }
}
