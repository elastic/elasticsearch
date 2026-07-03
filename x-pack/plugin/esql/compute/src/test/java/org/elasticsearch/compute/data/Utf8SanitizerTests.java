/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class Utf8SanitizerTests extends ESTestCase {

    private static final byte[] FFFD = { (byte) 0xEF, (byte) 0xBF, (byte) 0xBD };

    public void testWellFormedInputsAreReturnedUnchanged() {
        for (String s : new String[] { "", "ascii", "caf\u00e9", "\u20ac euro", "\ud83d\ude00 emoji", "mixed \u4e2d\u6587 text" }) {
            BytesRef in = new BytesRef(s);
            assertTrue(s, Utf8Sanitizer.isWellFormed(in.bytes, in.offset, in.length));
            BytesRef out = Utf8Sanitizer.sanitize(in);
            assertSame("well-formed input must not be copied", in, out);
        }
    }

    public void testNullIsPassedThrough() {
        assertNull(Utf8Sanitizer.sanitize(null));
    }

    public void testLoneContinuationByte() {
        assertSanitized(new byte[] { (byte) 0x80 }, FFFD);
        assertSanitized(new byte[] { 'a', (byte) 0xBF, 'b' }, concat(bytes('a'), FFFD, bytes('b')));
    }

    public void testHighLeadBytesF0xF8ToFF() {
        // These are the bytes that crash Utf8AscTopNEncoder's 248-entry table.
        for (int b = 0xF8; b <= 0xFF; b++) {
            assertSanitized(new byte[] { (byte) b }, FFFD);
        }
    }

    public void testTruncatedSequences() {
        // 2-byte lead with no continuation.
        assertSanitized(new byte[] { (byte) 0xC3 }, FFFD);
        // 3-byte lead with only one valid continuation: maximal subpart is 2 bytes -> single U+FFFD.
        assertSanitized(new byte[] { (byte) 0xE2, (byte) 0x82 }, FFFD);
        // 4-byte lead truncated after two continuations -> single U+FFFD.
        assertSanitized(new byte[] { (byte) 0xF0, (byte) 0x9F, (byte) 0x98 }, FFFD);
    }

    public void testOverlongEncodingsRejected() {
        // Overlong '/' (0x2F) as C0 AF and as E0 80 AF.
        assertSanitized(new byte[] { (byte) 0xC0, (byte) 0xAF }, concat(FFFD, FFFD));
        assertSanitized(new byte[] { (byte) 0xE0, (byte) 0x80, (byte) 0xAF }, concat(FFFD, FFFD, FFFD));
    }

    public void testSurrogateRangeRejected() {
        // U+D800 encoded as ED A0 80 is ill-formed in UTF-8.
        assertSanitized(new byte[] { (byte) 0xED, (byte) 0xA0, (byte) 0x80 }, concat(FFFD, FFFD, FFFD));
    }

    public void testValidMultibyteAdjacentToInvalid() {
        byte[] euro = "\u20ac".getBytes(StandardCharsets.UTF_8);
        byte[] input = concat(bytes('x'), euro, new byte[] { (byte) 0xFF }, euro, bytes('y'));
        byte[] expected = concat(bytes('x'), euro, FFFD, euro, bytes('y'));
        assertSanitized(input, expected);
    }

    public void testSanitizedOutputIsWellFormed() {
        for (int iter = 0; iter < 200; iter++) {
            byte[] random = new byte[randomIntBetween(0, 64)];
            random(random);
            BytesRef out = Utf8Sanitizer.sanitize(new BytesRef(random, 0, random.length));
            assertTrue("sanitized output must be valid UTF-8", Utf8Sanitizer.isWellFormed(out.bytes, out.offset, out.length));
        }
    }

    public void testRespectsOffsetAndLength() {
        byte[] backing = concat(new byte[] { (byte) 0xFF, (byte) 0xFF }, "ok".getBytes(StandardCharsets.UTF_8), new byte[] { (byte) 0xFF });
        BytesRef slice = new BytesRef(backing, 2, 2);
        BytesRef out = Utf8Sanitizer.sanitize(slice);
        assertSame(slice, out);
        assertEquals("ok", out.utf8ToString());
    }

    private void random(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) randomInt(255);
        }
    }

    private void assertSanitized(byte[] input, byte[] expected) {
        assertFalse("test input must be malformed", Utf8Sanitizer.isWellFormed(input, 0, input.length));
        BytesRef out = Utf8Sanitizer.sanitize(new BytesRef(input, 0, input.length));
        assertEquals(new BytesRef(expected), out);
        assertTrue(Utf8Sanitizer.isWellFormed(out.bytes, out.offset, out.length));
    }

    private static byte[] bytes(char c) {
        return new byte[] { (byte) c };
    }

    private static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) {
            len += p.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
