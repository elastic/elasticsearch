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
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

            assertThat(exception.getMessage(), containsString("Unexpected end-of-input"));
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
}
