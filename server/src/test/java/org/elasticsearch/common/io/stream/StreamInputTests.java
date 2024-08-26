/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

// Note: read* methods are tested for concrete implementations, this just covers helpers to read strings
public class StreamInputTests extends ESTestCase {

    private StreamInput in = Mockito.spy(StreamInput.class);

    {
        try {
            Mockito.when(in.skip(anyLong())).thenAnswer(a -> a.getArguments()[0]);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    byte[] bytes = "0123456789".getBytes(UTF_8);

    public void testCalculateByteLengthOfAscii() throws IOException {
        // not enough bytes to read all chars
        assertNull(in.tryReadStringFromBytes(bytes, 1, 10, 10));
        assertNull(in.tryReadStringFromBytes(bytes, 0, 9, 10));
        verify(in, never()).skip(anyLong());

        assertThat(in.tryReadStringFromBytes(bytes, 9, 10, 1), is("9"));
        verify(in).skip(1);
        clearInvocations(in);

        assertThat(in.tryReadStringFromBytes(bytes, 0, 10, 10), is("0123456789"));
        verify(in).skip(10);
    }

    public void testCalculateByteLengthOfNonAscii() throws IOException {
        // copy a two bytes char into bytes
        System.arraycopy("©".getBytes(UTF_8), 0, bytes, 0, 2);

        assertNull(in.tryReadStringFromBytes(bytes, 0, 1, 1));
        verify(in, never()).skip(anyLong());

        assertThat(in.tryReadStringFromBytes(bytes, 0, 2, 1), is("©"));
        verify(in).skip(2);
        clearInvocations(in);

        assertThat(in.tryReadStringFromBytes(bytes, 0, 10, 9), is("©23456789"));
        verify(in).skip(10);
        clearInvocations(in);

        // copy a three bytes char into bytes
        System.arraycopy("€".getBytes(UTF_8), 0, bytes, 0, 3);

        assertNull(in.tryReadStringFromBytes(bytes, 0, 2, 1));
        verify(in, never()).skip(anyLong());

        assertThat(in.tryReadStringFromBytes(bytes, 0, 3, 1), is("€"));
        verify(in).skip(3);
        clearInvocations(in);

        assertThat(in.tryReadStringFromBytes(bytes, 0, 10, 8), is("€3456789"));
        verify(in).skip(10);
        clearInvocations(in);

        // not enough bytes to read all chars
        assertNull(in.tryReadStringFromBytes(bytes, 0, 10, 9));
        verify(in, never()).skip(anyLong());
    }

    public void testCalculateByteLengthOfIncompleteNonAscii() throws IOException {
        // copy first byte to the end of bytes, this way the string can't ever be read completely
        System.arraycopy("©".getBytes(UTF_8), 0, bytes, 9, 1);

        assertThat(in.tryReadStringFromBytes(bytes, 8, 10, 1), is("8"));
        verify(in).skip(1);
        clearInvocations(in);

        assertNull(in.tryReadStringFromBytes(bytes, 9, 10, 1));
        verify(in, never()).skip(anyLong());

        // copy first two bytes of a three bytes char into bytes (similar to above)
        System.arraycopy("€".getBytes(UTF_8), 0, bytes, 8, 2);

        assertThat(in.tryReadStringFromBytes(bytes, 7, 10, 1), is("7"));
        verify(in).skip(1);
        clearInvocations(in);

        assertNull(in.tryReadStringFromBytes(bytes, 8, 10, 1));
        verify(in, never()).skip(anyLong());
    }

    public void testCalculateByteLengthOfSurrogate() throws IOException {
        BytesStreamOutput bytesOut = new BytesStreamOutput();
        bytesOut.writeString("ab💩");
        bytes = bytesOut.bytes.array();

        assertThat(bytes[0], is((byte) 4)); // 2+2 characters
        assertThat(in.tryReadStringFromBytes(bytes, 1, bytes.length, 2), is("ab"));
        verify(in).skip(2);
        clearInvocations(in);

        // surrogates use a special encoding, their byte length differs to what new String expects
        assertNull(in.tryReadStringFromBytes(bytes, 1, bytes.length, 4));
        assertNull(in.tryReadStringFromBytes(bytes, 3, bytes.length, 2));
        assertNull(in.tryReadStringFromBytes(bytes, 3, bytes.length, 1));
        verify(in, never()).skip(anyLong());

        // set limit so tight that we cannot read the first 3 byte char
        assertNull(in.tryReadStringFromBytes(bytes, 3, 5, 1));
        verify(in, never()).skip(anyLong());

        // if using the UTF-8 encoding, the surrogate pair is encoded as 4 bytes (rather than 2x 3 bytes)
        // this form of encoding isn't supported
        System.arraycopy("💩".getBytes(UTF_8), 0, bytes, 0, 4);
        assertThrows(IOException.class, () -> in.tryReadStringFromBytes(bytes, 0, bytes.length, 2));
        verify(in, never()).skip(anyLong());
    }
}
