/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class StreamsTests extends ESTestCase {
    public void testCopyFromInputStream() throws IOException {
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream in = new ByteArrayInputStream(content);
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        long count = Streams.copy(in, out);

        assertThat(count, equalTo((long) content.length));
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));
    }

    public void testCopyWithNestedStreamsReadOnSameThread() throws IOException {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(2, 5) * 8 * 1024);
        final ByteBuffer readBuffer = ByteBuffer.allocateDirect(8 * 1024);
        final InputStream in = new InputStream() {
            private final ByteArrayInputStream delegate = new ByteArrayInputStream(content);

            @Override
            public int read() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final int n = delegate.read(b, off, len);
                if (n > 0) {
                    readBuffer.clear();
                    // Will overwrite the copy buffer if copy/read share one ThreadLocal buffer
                    Streams.read(new ByteArrayInputStream(new byte[readBuffer.remaining()]), readBuffer, readBuffer.remaining());
                }
                return n;
            }
        };

        final ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        assertThat(Streams.copy(in, out, false), equalTo((long) content.length));
        assertArrayEquals(content, out.toByteArray());
    }
}
