/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class VectorDataTests extends ESTestCase {

    private static final float DELTA = 1e-5f;

    public void testThrowsIfBothVectorsAreNull() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new VectorData(null, null));
        assertThat(ex.getMessage(), containsString("please supply exactly either a float or a byte vector"));
    }

    public void testThrowsIfBothVectorsAreNonNull() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new VectorData(new float[] { 0f }, new byte[] { 1 })
        );
        assertThat(ex.getMessage(), containsString("please supply exactly either a float or a byte vector"));
    }

    public void testShouldCorrectlyConvertByteToFloatIfExplicitlyRequested() {
        byte[] byteVector = new byte[] { 1, 2, -127 };
        float[] expected = new float[] { 1f, 2f, -127f };

        VectorData vectorData = new VectorData(null, byteVector);
        float[] actual = vectorData.asFloatVector();
        assertArrayEquals(expected, actual, DELTA);
    }

    public void testShouldThrowForDecimalsWhenConvertingToByte() {
        float[] vec = new float[] { 1f, 2f, 3.1f };

        VectorData vectorData = new VectorData(vec, null);
        expectThrows(IllegalArgumentException.class, vectorData::asByteVector);
    }

    public void testShouldThrowForOutsideRangeWhenConvertingToByte() {
        float[] vec = new float[] { 1f, 2f, 200f };

        VectorData vectorData = new VectorData(vec, null);
        expectThrows(IllegalArgumentException.class, vectorData::asByteVector);
    }

    public void testEqualsAndHashCode() {
        VectorData v1 = new VectorData(new float[] { 1, 2, 3 }, null);
        VectorData v2 = new VectorData(null, new byte[] { 1, 2, 3 });
        assertNotEquals(v1, v2);
        assertNotEquals(v1.hashCode(), v2.hashCode());

        VectorData v3 = new VectorData(null, new byte[] { 1, 2, 3 });
        assertEquals(v2, v3);
        assertEquals(v2.hashCode(), v3.hashCode());
    }

    public void testParseHexCorrectly() throws IOException {
        byte[] expected = new byte[] { 64, 10, -30, 10 };
        String toParse = "\"400ae20a\"";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            assertArrayEquals(expected, parsed.asByteVector());
        }
    }

    public void testParseFloatArray() throws IOException {
        float[] expected = new float[] { 1f, -1f, .1f };
        String toParse = "[1.0, -1.0, 0.1]";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            assertArrayEquals(expected, parsed.asFloatVector(), DELTA);
        }
    }

    public void testParseByteArray() throws IOException {
        byte[] expected = new byte[] { 64, 10, -30, 10 };
        String toParse = "[64,10,-30,10]";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            assertArrayEquals(expected, parsed.asByteVector());
        }
    }

    public void testByteThrowsForOutsideRange() throws IOException {
        String toParse = "[1000]";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, parsed::asByteVector);
            assertThat(ex.getMessage(), containsString("vectors only support integers between [-128, 127]"));
        }
    }

    public void testAsByteThrowsForDecimals() throws IOException {
        String toParse = "[0.1]";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, parsed::asByteVector);
            assertThat(ex.getMessage(), containsString("vectors only support non-decimal values but found decimal value"));
        }
    }

    public void testParseSingleNumber() throws IOException {
        float[] expected = new float[] { 0.1f };
        String toParse = "0.1";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            VectorData parsed = VectorData.parseXContent(parser);
            assertArrayEquals(expected, parsed.asFloatVector(), DELTA);
        }
    }

    public void testParseThrowsForUnknown() throws IOException {
        String unknown = "{\"foo\":\"bar\"}";
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                new BytesArray(unknown),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> VectorData.parseXContent(parser));
            assertThat(ex.getMessage(), containsString("Unknown type [" + XContentParser.Token.START_OBJECT + "] for parsing vector"));
        }
    }

    public void testFailForUnknownArrayValue() throws IOException {
        String toParse = "[0.1, true]";
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                new BytesArray(toParse),
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            ParsingException ex = expectThrows(ParsingException.class, () -> VectorData.parseXContent(parser));
            assertThat(ex.getMessage(), containsString("Type [" + XContentParser.Token.VALUE_BOOLEAN + "] not supported for query vector"));
        }
    }
}
