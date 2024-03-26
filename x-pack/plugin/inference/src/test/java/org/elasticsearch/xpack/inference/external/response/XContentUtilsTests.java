/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;

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
}
