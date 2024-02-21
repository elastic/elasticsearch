/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public void testThrowsWhenTryingToConvertByteToFloat() {
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> {
            VectorData vectorData = new VectorData(null, new byte[] { 1, 2, -127 });
            vectorData.asFloatVector();
        });
        assertThat(ex.getMessage(), containsString("cannot convert to float, as we're explicitly using a byte vector"));
    }

    public void testShouldCorrectlyConvertByteToFloatIfExplicitlyRequested() {
        byte[] byteVector = new byte[] { 1, 2, -127 };
        float[] expected = new float[] { 1f, 2f, -127f };

        VectorData vectorData = new VectorData(null, byteVector);
        float[] actual = vectorData.asFloatVector(false);
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
        VectorData expected = new VectorData(null, new byte[] { 64, 10, -30, 10 });
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.VALUE_STRING);
        when(parserMock.text()).thenReturn("400ae20a");
        VectorData parsed = VectorData.parseXContent(parserMock);
        assertEquals(expected, parsed);
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, parsed::asFloatVector);
        assertThat(ex.getMessage(), containsString("cannot convert to float, as we're explicitly using a byte vector"));
    }

    public void testParseFloatArray() throws IOException {
        VectorData expected = new VectorData(new float[] { 1f, -1f, .1f }, null);
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_ARRAY);
        when(parserMock.nextToken()).thenReturn(
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.END_ARRAY
        );
        when(parserMock.floatValue()).thenReturn(1f, -1f, .1f);
        VectorData parsed = VectorData.parseXContent(parserMock);
        assertEquals(expected, parsed);
    }

    public void testParseByteArray() throws IOException {
        byte[] expectedByteArray = new byte[] { 64, 10, -30, 10 };
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_ARRAY);
        when(parserMock.nextToken()).thenReturn(
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.END_ARRAY
        );
        when(parserMock.floatValue()).thenReturn(64f, 10f, -30f, 10f);
        VectorData parsed = VectorData.parseXContent(parserMock);
        assertArrayEquals(expectedByteArray, parsed.asByteVector());
    }

    public void testByteThrowsForOutsideRange() throws IOException {
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_ARRAY);
        when(parserMock.nextToken()).thenReturn(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.END_ARRAY);
        when(parserMock.floatValue()).thenReturn(1000f);
        VectorData parsed = VectorData.parseXContent(parserMock);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, parsed::asByteVector);
        assertThat(ex.getMessage(), containsString("vectors only support integers between [-128, 127]"));
    }

    public void testAsByteThrowsForDecimals() throws IOException {
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_ARRAY);
        when(parserMock.nextToken()).thenReturn(XContentParser.Token.VALUE_NUMBER, XContentParser.Token.END_ARRAY);
        when(parserMock.floatValue()).thenReturn(0.1f);
        VectorData parsed = VectorData.parseXContent(parserMock);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, parsed::asByteVector);
        assertThat(ex.getMessage(), containsString("vectors only support non-decimal values but found decimal value"));
    }

    public void testParseSingleNumber() throws IOException {
        VectorData expected = new VectorData(new float[] { 0.1f }, null);
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.VALUE_NUMBER);
        when(parserMock.floatValue()).thenReturn(0.1f);
        VectorData parsed = VectorData.parseXContent(parserMock);
        assertEquals(expected, parsed);
    }

    public void testParseThrowsForUnknown() {
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        ParsingException ex = expectThrows(ParsingException.class, () -> VectorData.parseXContent(parserMock));
        assertThat(ex.getMessage(), containsString("Unknown type [" + XContentParser.Token.START_OBJECT + "] for parsing vector"));
    }

    public void testFailForUnknownArrayValue() throws IOException {
        VectorData expected = new VectorData(new float[] { 1f, -1f, .1f }, null);
        XContentParser parserMock = mock(XContentParser.class);
        when(parserMock.currentToken()).thenReturn(XContentParser.Token.START_ARRAY);
        when(parserMock.nextToken()).thenReturn(
            XContentParser.Token.VALUE_NUMBER,
            XContentParser.Token.VALUE_BOOLEAN,
            XContentParser.Token.END_ARRAY
        );
        when(parserMock.floatValue()).thenReturn(1f, -1f, .1f);
        ParsingException ex = expectThrows(ParsingException.class, () -> VectorData.parseXContent(parserMock));
        assertThat(ex.getMessage(), containsString("Type [" + XContentParser.Token.VALUE_BOOLEAN + "] not supported for query vector"));

    }
}
