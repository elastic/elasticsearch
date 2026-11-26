/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.simdjson.SimdJsonTestCase.appendRandomStringContent;
import static org.elasticsearch.simdjson.SimdJsonTestCase.copyMismatch;

public class Utf8ValueTests extends ESTestCase {

    public void testMaterializedValueCopiesItsSlice() {
        byte[] source = randomByteArrayOfLength(between(0, 200));
        int offset = between(0, source.length);
        int length = between(0, source.length - offset);
        Utf8Value value = Utf8Value.of(source, offset, length);
        assertEquals(length, value.maxLength());

        byte[] dst = new byte[between(0, 50) + value.maxLength()];
        int dstOffset = between(0, dst.length - value.maxLength());
        assertEquals(length, value.copyTo(dst, dstOffset));
        assertArrayEquals(Arrays.copyOfRange(source, offset, offset + length), Arrays.copyOfRange(dst, dstOffset, dstOffset + length));
    }

    /** A raw value bounded as the walker bounds it, up to the structural character after the string, whitespace included. */
    public void testRawValueUnescapesWhileCopying() {
        assumeTrue("raw values are copied by simdjson's string parser", SimdJsonSupport.isSupported());
        StringBuilder expected = new StringBuilder();
        StringBuilder json = new StringBuilder();
        appendRandomStringContent(expected, json);
        String after = randomFrom("", " ", "\n  ") + randomFrom(",", "}", "]") + randomAlphaOfLength(between(0, 70));
        assertNull(
            copyMismatch(
                expected.toString(),
                json.toString(),
                between(0, 70),
                after,
                between(0, 50),
                (buffer, quoteIdx, maxLength, dst, dstOffset) -> new RawJsonUtf8Value(buffer, quoteIdx, maxLength).copyTo(dst, dstOffset)
            )
        );
    }

    public void testRawValueThrowsOnMalformedEscape() {
        assumeTrue("raw values are copied by simdjson's string parser", SimdJsonSupport.isSupported());
        String json = randomAlphaOfLength(between(0, 70)) + randomFrom(
            "\\q" + randomAlphaOfLength(between(0, 70)),
            "\\u12G4" + randomAlphaOfLength(between(0, 70)),
            "\\u12" // cut short by the closing quote
        );
        byte[] buffer = ('"' + json + "\",").getBytes(UTF_8);
        Utf8Value value = new RawJsonUtf8Value(buffer, 0, json.length());
        byte[] dst = new byte[value.maxLength() + SimdJsonSupport.DESTINATION_PADDING];
        expectThrows(JsonParsingException.class, () -> value.copyTo(dst, 0));
    }
}
