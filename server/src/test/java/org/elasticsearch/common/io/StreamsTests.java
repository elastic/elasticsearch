/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.common.io.Streams.copyToString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link org.elasticsearch.common.io.Streams}.
 */
public class StreamsTests extends ESTestCase {

    public void testCopyFromByteArray() throws IOException {
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        Streams.copy(content, out);
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));
    }

    public void testCopyFromReader() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        StringWriter out = new StringWriter();
        int count = Streams.copy(in, out);
        assertThat(content.length(), equalTo(count));
        assertThat(out.toString(), equalTo(content));
    }

    public void testCopyFromString() throws IOException {
        String content = "content";
        StringWriter out = new StringWriter();
        Streams.copy(content, out);
        assertThat(out.toString(), equalTo(content));
    }

    public void testCopyToString() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        String result = copyToString(in);
        assertThat(result, equalTo(content));
    }

    public void testBytesStreamInput() throws IOException {
        byte stuff[] = new byte[] { 0, 1, 2, 3 };
        BytesRef stuffRef = new BytesRef(stuff, 2, 2);
        BytesArray stuffArray = new BytesArray(stuffRef);
        StreamInput input = stuffArray.streamInput();
        assertEquals(2, input.read());
        assertEquals(3, input.read());
        assertEquals(-1, input.read());
        input.close();
    }

    public void testFullyConsumeInputStream() throws IOException {
        final String bytes = randomAlphaOfLengthBetween(0, 100);
        final BytesArray stuffArray = new BytesArray(bytes);
        assertEquals(bytes.length(), Streams.consumeFully(stuffArray.streamInput()));
    }

    public void testLimitInputStream() throws IOException {
        final byte[] bytes = randomAlphaOfLengthBetween(1, 100).getBytes(StandardCharsets.UTF_8);
        final int limit = randomIntBetween(0, bytes.length);
        final BytesArray stuffArray = new BytesArray(bytes);
        final ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
        final long count = org.elasticsearch.core.internal.io.Streams.copy(Streams.limitStream(stuffArray.streamInput(), limit), out);
        assertEquals(limit, count);
        assertThat(Arrays.equals(out.toByteArray(), Arrays.copyOf(bytes, limit)), equalTo(true));
    }
}
