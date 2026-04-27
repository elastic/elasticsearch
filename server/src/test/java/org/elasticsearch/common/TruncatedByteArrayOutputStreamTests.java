/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TruncatedByteArrayOutputStreamTests extends ESTestCase {

    public void testUnderLimit() throws IOException {
        var out = new TruncatedByteArrayOutputStream(1024);

        out.write('a');
        out.write(new byte[] { 'b', 'c', 'd' });
        out.write(new byte[] { 'd', 'e', 'f', 'g', 'h' }, 1, 3);

        assertFalse(out.isOverLimit());
        assertEquals("abcdefg", out.toString(StandardCharsets.UTF_8));
    }

    public void testOverLimit() throws IOException {
        var out = new TruncatedByteArrayOutputStream(10);
        out.write("1234567890".getBytes(StandardCharsets.UTF_8));

        out.write('a');
        out.write(new byte[] { 'b', 'c', 'd' });
        out.write(new byte[] { 'd', 'e', 'f', 'g', 'h' }, 1, 3);

        assertTrue(out.isOverLimit());
        assertEquals("1234567890", out.toString(StandardCharsets.UTF_8));
    }

    public void testBreaksInTheMiddleOf4ByteUtf8Character() throws IOException {
        var out = new TruncatedByteArrayOutputStream(3);

        out.write("\uD83C\uDF4E".getBytes(StandardCharsets.UTF_8)); // red apple emoji🍎

        // UTF-8 CharsetDecoder replaces invalid UTF-8 codepoints with \uFFFD
        assertEquals("\uFFFD", out.toString(StandardCharsets.UTF_8));
    }
}
