/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.common.io.stream.StreamInput.calculateByteLengthOfChars;
import static org.hamcrest.Matchers.is;

// Note: read* methods are tested for concrete implementations, this just contains tests for some static utilities
public class StreamInputTests extends ESTestCase {

    public void testCalculateByteLengthOfAscii() throws IOException {
        byte[] bytes = randomAlphaOfLength(10).getBytes(UTF_8);

        assertThat(calculateByteLengthOfChars(bytes, 1, 9, 10), is(1));
        assertThat(calculateByteLengthOfChars(bytes, 10, 0, 10), is(10));

        // not enough bytes to read all chars
        assertThat(calculateByteLengthOfChars(bytes, 10, 1, 10), is(-1));
        assertThat(calculateByteLengthOfChars(bytes, 10, 0, 9), is(-1));
    }

    public void testCalculateByteLengthOfNonAscii() throws IOException {
        byte[] bytes = randomAlphaOfLength(10).getBytes(UTF_8);
        // copy a two bytes char into bytes
        System.arraycopy("©".getBytes(UTF_8), 0, bytes, 0, 2);

        assertThat(calculateByteLengthOfChars(bytes, 1, 0, 1), is(-1));
        assertThat(calculateByteLengthOfChars(bytes, 1, 0, 2), is(2));
        assertThat(calculateByteLengthOfChars(bytes, 9, 0, 10), is(10));

        // copy a three bytes char into bytes
        System.arraycopy("€".getBytes(UTF_8), 0, bytes, 0, 3);

        assertThat(calculateByteLengthOfChars(bytes, 1, 0, 2), is(-1));
        assertThat(calculateByteLengthOfChars(bytes, 1, 0, 3), is(3));
        assertThat(calculateByteLengthOfChars(bytes, 8, 0, 10), is(10));

        // not enough bytes to read all chars
        assertThat(calculateByteLengthOfChars(bytes, 9, 0, 10), is(-1));
    }

    public void testCalculateByteLengthOfIncompleteNonAscii() throws IOException {
        byte[] bytes = randomAlphaOfLength(10).getBytes(UTF_8);
        // copy first byte to the end of bytes, this way the string can't ever be read completely
        System.arraycopy("©".getBytes(UTF_8), 0, bytes, 9, 1);

        assertThat(calculateByteLengthOfChars(bytes, 1, 8, 10), is(1));
        assertThat(calculateByteLengthOfChars(bytes, 1, 9, 10), is(-1));

        // copy first two bytes of a three bytes char into bytes (similar to above)
        System.arraycopy("€".getBytes(UTF_8), 0, bytes, 8, 2);

        assertThat(calculateByteLengthOfChars(bytes, 1, 7, 10), is(1));
        assertThat(calculateByteLengthOfChars(bytes, 1, 8, 10), is(-1));
    }

    public void testCalculateByteLengthOfSurrogate() throws IOException {
        BytesStreamOutput bytesOut = new BytesStreamOutput();
        bytesOut.writeString("ab💩");
        byte[] bytes = bytesOut.bytes.array();

        assertThat(bytes[0], is((byte) 4)); // 2+2 characters
        assertThat(calculateByteLengthOfChars(bytes, 2, 1, bytes.length), is(2));

        // surrogates use a special encoding, their byte length differs to what new String expects
        assertThat(calculateByteLengthOfChars(bytes, 4, 1, bytes.length), is(-1));
        assertThat(calculateByteLengthOfChars(bytes, 2, 3, bytes.length), is(-1));
        assertThat(calculateByteLengthOfChars(bytes, 1, 3, bytes.length), is(-1));

        // set limit so tight that we cannot read the first 3 byte char
        assertThat(calculateByteLengthOfChars(bytes, 1, 3, 5), is(-1));

        // if using the UTF-8 encoding, the surrogate pair is encoded as 4 bytes (rather than 2x 3 bytes)
        // this form of encoding isn't supported
        System.arraycopy("💩".getBytes(UTF_8), 0, bytes, 0, 4);
        assertThrows(IOException.class, () -> calculateByteLengthOfChars(bytes, 2, 0, bytes.length));
    }
}
