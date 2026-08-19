/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;

public class EirfRowXContentParserTests extends ESTestCase {

    // EIRF-specific: EirfEncoder only produces top-level KEY_VALUE leaves for empty objects;
    // non-empty objects are flattened into the schema tree. EirfRowBuilder is used here to
    // hand-construct a non-empty KEY_VALUE column, exercising the KV-walk path that ESCF
    // cannot reach.
    public void testPositionAtLeafValueNonEmptyKeyValue() throws IOException {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            byte[] kvBytes = new byte[] {
                // entry 1: key="x" (i32 LE length=1), INT value=42
                1,
                0,
                0,
                0,
                'x',
                SourceValueType.INT,
                42,
                0,
                0,
                0,
                // entry 2: key="s" (i32 LE length=1), STRING value="hi" (i32 LE length=2)
                1,
                0,
                0,
                0,
                's',
                SourceValueType.STRING,
                2,
                0,
                0,
                0,
                'h',
                'i' };
            builder.startDocument();
            builder.setKeyValue("data", kvBytes);
            builder.endDocument();

            try (EirfBatch batch = builder.build()) {
                SourceSchema schema = batch.schema();
                EirfRowReader row = batch.getRowReader(0);
                SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(schema);
                try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, row)) {
                    int dataLeaf = schema.findLeaf("data", 0);
                    assertEquals(Token.START_OBJECT, parser.positionAtLeafValue(dataLeaf));
                    assertFieldName(parser, "x");
                    assertToken(parser, Token.VALUE_NUMBER);
                    assertEquals(42, parser.intValue());
                    assertFieldName(parser, "s");
                    assertToken(parser, Token.VALUE_STRING);
                    assertEquals("hi", parser.text());
                    assertToken(parser, Token.END_OBJECT);
                    assertNull(parser.nextToken());
                }
            }
        }
    }

    private static void assertToken(SourceRowXContentParser parser, Token expected) throws IOException {
        assertEquals(expected, parser.nextToken());
    }

    private static void assertFieldName(SourceRowXContentParser parser, String expected) throws IOException {
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected, parser.currentName());
    }
}
