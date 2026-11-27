/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
            byte[] random = randomByteArrayOfLength(randomIntBetween(0, 64));
            BytesRef out = Utf8Sanitizer.sanitize(new BytesRef(random, 0, random.length));
            assertTrue("sanitized output must be valid UTF-8", Utf8Sanitizer.isWellFormed(out.bytes, out.offset, out.length));
        }
    }

    public void testLongAsciiRunIsWellFormed() {
        // Exercises the word-wide fast path over several full 8-byte words plus a partial tail.
        for (int len : new int[] { 7, 8, 9, 15, 16, 17, 63, 64, 65 }) {
            byte[] ascii = new byte[len];
            Arrays.fill(ascii, (byte) 'a');
            BytesRef in = new BytesRef(ascii);
            assertTrue("len=" + len, Utf8Sanitizer.isWellFormed(in.bytes, in.offset, in.length));
            assertSame(in, Utf8Sanitizer.sanitize(in));
        }
    }

    public void testMalformedByteAtEveryWordPosition() {
        // A single ill-formed lead byte is detected (and repaired) no matter where it falls
        // relative to an 8-byte fast-path word, including exactly on a word boundary.
        int len = 20;
        for (int pos = 0; pos < len; pos++) {
            byte[] bytes = new byte[len];
            Arrays.fill(bytes, (byte) 'a');
            bytes[pos] = (byte) 0xFF;
            byte[] expected = concat(repeat((byte) 'a', pos), FFFD, repeat((byte) 'a', len - pos - 1));
            assertSanitized(bytes, expected);
        }
    }

    private static byte[] repeat(byte b, int count) {
        byte[] out = new byte[count];
        Arrays.fill(out, b);
        return out;
    }

    public void testRespectsOffsetAndLength() {
        byte[] backing = concat(new byte[] { (byte) 0xFF, (byte) 0xFF }, "ok".getBytes(StandardCharsets.UTF_8), new byte[] { (byte) 0xFF });
        BytesRef slice = new BytesRef(backing, 2, 2);
        BytesRef out = Utf8Sanitizer.sanitize(slice);
        assertSame(slice, out);
        assertEquals("ok", out.utf8ToString());
    }

    public void testIsWellFormedMatchesSequenceLengthWalkRandom() {
        for (int iter = 0; iter < 400; iter++) {
            int pad = randomIntBetween(0, 8);
            int len = randomIntBetween(0, 300);
            int tail = randomIntBetween(0, 8);
            byte[] buf = randomByteArrayOfLength(pad + len + tail);
            Arrays.fill(buf, 0, pad, (byte) 0xFF);
            Arrays.fill(buf, pad + len, buf.length, (byte) 0xFF);
            assertIsWellFormedMatchesOracles(buf, pad, len);
        }
    }

    public void testIsWellFormedMatchesSequenceLengthWalkBiasedMultibyte() {
        for (int iter = 0; iter < 200; iter++) {
            byte[] payload = randomBiasedUtf8Payload();
            int pad = randomIntBetween(1, 8);
            byte[] buf = new byte[pad + payload.length + pad];
            Arrays.fill(buf, (byte) 0xFF);
            System.arraycopy(payload, 0, buf, pad, payload.length);
            assertIsWellFormedMatchesOracles(buf, pad, payload.length);
            if (walkSequenceLength(buf, pad, payload.length)) {
                BytesRef in = new BytesRef(buf, pad, payload.length);
                assertSame("well-formed input must not be copied", in, Utf8Sanitizer.sanitize(in));
            }
        }
    }

    public void testExhaustiveOneByteIsWellFormed() {
        byte[] buf = new byte[3];
        buf[0] = (byte) 0xFF;
        buf[2] = (byte) 0xFF;
        for (int b = 0; b < 256; b++) {
            buf[1] = (byte) b;
            assertIsWellFormedMatchesOracles(buf, 1, 1);
            assertEquals("b=" + b, b < 0x80, Utf8Sanitizer.isWellFormed(buf, 1, 1));
        }
    }

    public void testExhaustiveTwoByteIsWellFormed() {
        byte[] buf = new byte[4];
        buf[0] = (byte) 0xFF;
        buf[3] = (byte) 0xFF;
        for (int b0 = 0; b0 < 256; b0++) {
            buf[1] = (byte) b0;
            for (int b1 = 0; b1 < 256; b1++) {
                buf[2] = (byte) b1;
                assertIsWellFormedMatchesOracles(buf, 1, 2);
            }
        }
    }

    public void testExhaustiveThreeByteUnambiguousLeads() {
        // E1..EC / EE / EF are the 3-byte run. E0/ED stay in sequenceLength (tested in special leads).
        byte[] buf = new byte[5];
        buf[0] = (byte) 0xFF;
        buf[4] = (byte) 0xFF;
        for (int b0 = 0xE1; b0 <= 0xEF; b0++) {
            if (b0 == 0xED) {
                continue;
            }
            buf[1] = (byte) b0;
            for (int b1 = 0; b1 < 256; b1++) {
                buf[2] = (byte) b1;
                for (int b2 = 0; b2 < 256; b2++) {
                    buf[3] = (byte) b2;
                    assertIsWellFormedMatchesOracles(buf, 1, 3);
                }
            }
        }
    }

    public void testIsAllAscii() {
        assertTrue(Utf8Sanitizer.isAllAscii(new byte[0], 0, 0));
        byte[] shortAscii = new byte[63];
        Arrays.fill(shortAscii, (byte) 'a');
        assertTrue(Utf8Sanitizer.isAllAscii(shortAscii, 0, 63));
        byte[] block = new byte[64];
        Arrays.fill(block, (byte) 'a');
        assertTrue(Utf8Sanitizer.isAllAscii(block, 0, 64));
        block[0] = (byte) 0xFF;
        assertFalse(Utf8Sanitizer.isAllAscii(block, 0, 64));
        block[0] = (byte) 'a';
        block[63] = (byte) 0xFF;
        assertFalse(Utf8Sanitizer.isAllAscii(block, 0, 64));
        byte[] cafe = "café".getBytes(StandardCharsets.UTF_8);
        assertFalse(Utf8Sanitizer.isAllAscii(cafe, 0, cafe.length));
    }

    public void testIsPackedWellFormed() {
        byte[] ascii = "ab".getBytes(StandardCharsets.UTF_8);
        assertTrue(Utf8Sanitizer.isPackedWellFormed(ascii, 0, ascii.length, new int[] { 1, 2 }, 2));
        byte[] cafe = "café".getBytes(StandardCharsets.UTF_8);
        assertTrue(Utf8Sanitizer.isPackedWellFormed(cafe, 0, cafe.length, new int[] { cafe.length }, 1));
        byte[] emoji = "\uD83D\uDE00".getBytes(StandardCharsets.UTF_8);
        assertTrue(Utf8Sanitizer.isPackedWellFormed(emoji, 0, emoji.length, new int[] { emoji.length }, 1));
        byte[] split = { (byte) 0xC3, (byte) 0xA9 };
        assertTrue(Utf8Sanitizer.isWellFormed(split, 0, 2));
        assertFalse(Utf8Sanitizer.isPackedWellFormed(split, 0, 2, new int[] { 1, 2 }, 2));
        byte[] bad = { (byte) 0xFF, 'a' };
        assertFalse(Utf8Sanitizer.isPackedWellFormed(bad, 0, 2, new int[] { 1, 2 }, 2));
        assertTrue(Utf8Sanitizer.isPackedWellFormed(new byte[0], 0, 0, new int[] { 0, 0 }, 2));
    }

    public void testAsciiBlockThenTwoByteRunThenAscii() {
        // After the 2-byte run, ASCII skip must resume at that offset, not the next 64-byte
        // boundary. Trailing ASCII is long enough that skipAsciiBlocks actually runs again.
        byte[] buf = new byte[64 + 4 + 64];
        Arrays.fill(buf, (byte) 'a');
        buf[64] = (byte) 0xD0;
        buf[65] = (byte) 0x90;
        buf[66] = (byte) 0xD0;
        buf[67] = (byte) 0x91;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        BytesRef in = new BytesRef(buf);
        assertSame(in, Utf8Sanitizer.sanitize(in));

        // A 0xFF sitting in the gap before the next 64-byte boundary must still be detected.
        buf[68] = (byte) 0xFF;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        assertFalse(Utf8Sanitizer.isWellFormed(buf, 0, buf.length));
    }

    public void testAsciiBlockThenThreeByteRunThenAscii() {
        // Same resume-after-run contract as the 2-byte test, for the CJK 3-byte kernel.
        byte[] buf = new byte[64 + 3 + 64];
        Arrays.fill(buf, (byte) 'a');
        buf[64] = (byte) 0xE3;
        buf[65] = (byte) 0x80;
        buf[66] = (byte) 0x80;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        BytesRef in = new BytesRef(buf);
        assertSame(in, Utf8Sanitizer.sanitize(in));

        buf[67] = (byte) 0xFF;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        assertFalse(Utf8Sanitizer.isWellFormed(buf, 0, buf.length));
    }

    public void testAsciiBlockThenFourByteRunThenAscii() {
        // F0 (emoji U+1F600 = F0 9F 98 80) plus F1..F3. F0 overlong / F4 stay in sequenceLength.
        byte[] grin = "\uD83D\uDE00".getBytes(StandardCharsets.UTF_8);
        assertEquals(4, grin.length);
        assertEquals((byte) 0xF0, grin[0]);
        byte[] buf = new byte[64 + grin.length + 4 + 64];
        Arrays.fill(buf, (byte) 'a');
        System.arraycopy(grin, 0, buf, 64, grin.length);
        buf[64 + grin.length] = (byte) 0xF1;
        buf[64 + grin.length + 1] = (byte) 0x80;
        buf[64 + grin.length + 2] = (byte) 0x80;
        buf[64 + grin.length + 3] = (byte) 0x80;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        BytesRef in = new BytesRef(buf);
        assertSame(in, Utf8Sanitizer.sanitize(in));

        buf[64 + grin.length + 4] = (byte) 0xFF;
        assertIsWellFormedMatchesOracles(buf, 0, buf.length);
        assertFalse(Utf8Sanitizer.isWellFormed(buf, 0, buf.length));
    }

    public void testNonAsciiAtEveryOffsetInAsciiBlock() {
        int len = 64;
        for (int pos = 0; pos < len; pos++) {
            byte[] bytes = new byte[len];
            Arrays.fill(bytes, (byte) 'a');
            bytes[pos] = (byte) 0xFF;
            assertIsWellFormedMatchesOracles(bytes, 0, len);
            assertFalse(Utf8Sanitizer.isWellFormed(bytes, 0, len));
        }
    }

    public void testPlainPackedRegionWithBadValueInMiddleIsMalformed() {
        // [int32 LE 2][aa][int32 LE 1][0xFF][int32 LE 2][cc] — prefixes are ASCII, payload is not.
        byte[] packed = new byte[] { 2, 0, 0, 0, 'a', 'a', 1, 0, 0, 0, (byte) 0xFF, 2, 0, 0, 0, 'c', 'c' };
        assertFalse(Utf8Sanitizer.isWellFormed(packed, 0, packed.length));
        assertIsWellFormedMatchesOracles(packed, 0, packed.length);
        assertEquals(new BytesRef("aa"), Utf8Sanitizer.sanitize(new BytesRef(packed, 4, 2)));
        assertEquals(new BytesRef("\uFFFD"), Utf8Sanitizer.sanitize(new BytesRef(packed, 10, 1)));
        assertEquals(new BytesRef("cc"), Utf8Sanitizer.sanitize(new BytesRef(packed, 15, 2)));
    }

    public void testSpecialLeadsMatchOraclesExhaustively() {
        // E0/ED (overlong/surrogate) need a three-byte slice so a valid second byte is not
        // masked by a trailing dummy continuation. 4-byte leads stay a four-byte slice.
        byte[] buf = new byte[6];
        buf[0] = (byte) 0xFF;
        buf[5] = (byte) 0xFF;
        for (int lead : new int[] { 0xE0, 0xED }) {
            buf[1] = (byte) lead;
            for (int b1 = 0; b1 < 256; b1++) {
                buf[2] = (byte) b1;
                for (int b2 = 0; b2 < 256; b2++) {
                    buf[3] = (byte) b2;
                    assertIsWellFormedMatchesOracles(buf, 1, 3);
                }
            }
        }
        for (int lead : new int[] { 0xF0, 0xF4, 0xF5 }) {
            buf[1] = (byte) lead;
            for (int b1 = 0; b1 < 256; b1++) {
                buf[2] = (byte) b1;
                for (int b2 = 0; b2 < 256; b2++) {
                    buf[3] = (byte) b2;
                    buf[4] = (byte) 0x80;
                    assertIsWellFormedMatchesOracles(buf, 1, 4);
                }
            }
        }
    }

    private static void assertIsWellFormedMatchesOracles(byte[] bytes, int off, int len) {
        boolean fast = Utf8Sanitizer.isWellFormed(bytes, off, len);
        boolean walked = walkSequenceLength(bytes, off, len);
        boolean jdk = jdkWellFormed(bytes, off, len);
        assertEquals("isWellFormed must match sequenceLength walk; off=" + off + " len=" + len, walked, fast);
        assertEquals("isWellFormed must match JDK UTF-8 decoder; off=" + off + " len=" + len, jdk, fast);
    }

    /**
     * Fast-path vs slow-path differential: walk {@link Utf8Sanitizer#sequenceLength}. Not a copy
     * of the 64-byte/run kernel, but shares {@code sequenceLength} with production.
     */
    private static boolean walkSequenceLength(byte[] bytes, int off, int len) {
        int i = off;
        int end = off + len;
        while (i < end) {
            int n = Utf8Sanitizer.sequenceLength(bytes, i, end);
            if (n < 0) {
                return false;
            }
            i += n;
        }
        return true;
    }

    /** Independent well-formedness oracle: JDK UTF-8 decoder in REPORT mode. */
    private static boolean jdkWellFormed(byte[] bytes, int off, int len) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            decoder.decode(ByteBuffer.wrap(bytes, off, len));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    private byte[] randomBiasedUtf8Payload() {
        return switch (randomIntBetween(0, 9)) {
            case 0 -> randomValidTwoByteRun(randomIntBetween(1, 40));
            case 1 -> randomValidThreeByteRun(randomIntBetween(1, 24));
            case 2 -> concat(randomAscii(randomIntBetween(0, 80)), randomValidTwoByteRun(randomIntBetween(1, 16)));
            case 3 -> concat(randomValidTwoByteRun(randomIntBetween(1, 16)), randomAscii(randomIntBetween(1, 80)));
            case 4 -> concat(
                randomAscii(randomIntBetween(0, 70)),
                randomValidTwoByteRun(randomIntBetween(1, 8)),
                randomAscii(randomIntBetween(0, 16)),
                randomValidThreeByteRun(randomIntBetween(1, 8))
            );
            case 5 -> randomValidFourByteRun(randomIntBetween(1, 8));
            case 6 -> randomValidE0Run(randomIntBetween(1, 8));
            case 7 -> randomValidEdRun(randomIntBetween(1, 8));
            case 8 -> randomValidUnambiguousFourByteRun(randomIntBetween(1, 8));
            case 9 -> {
                byte[] run = switch (randomIntBetween(0, 4)) {
                    case 0 -> randomValidTwoByteRun(randomIntBetween(1, 8));
                    case 1 -> randomValidThreeByteRun(randomIntBetween(1, 8));
                    case 2 -> randomValidFourByteRun(randomIntBetween(1, 8));
                    case 3 -> randomValidE0Run(randomIntBetween(1, 8));
                    case 4 -> randomValidUnambiguousFourByteRun(randomIntBetween(1, 8));
                    default -> throw new AssertionError("unexpected junk-run arm");
                };
                run[randomInt(run.length - 1)] = (byte) randomInt(255);
                yield concat(randomAscii(randomIntBetween(0, 64)), run);
            }
            default -> throw new AssertionError("unexpected bias arm");
        };
    }

    private byte[] randomAscii(int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) randomIntBetween(0, 0x7F);
        }
        return out;
    }

    private byte[] randomValidTwoByteRun(int chars) {
        byte[] out = new byte[chars * 2];
        for (int i = 0; i < chars; i++) {
            out[i * 2] = (byte) randomIntBetween(0xC2, 0xDF);
            out[i * 2 + 1] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
    }

    private byte[] randomValidThreeByteRun(int chars) {
        byte[] out = new byte[chars * 3];
        for (int i = 0; i < chars; i++) {
            int lead = randomBoolean() ? randomIntBetween(0xE1, 0xEC) : randomIntBetween(0xEE, 0xEF);
            out[i * 3] = (byte) lead;
            out[i * 3 + 1] = (byte) randomIntBetween(0x80, 0xBF);
            out[i * 3 + 2] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
    }

    private byte[] randomValidFourByteRun(int chars) {
        byte[] out = new byte[chars * 4];
        for (int i = 0; i < chars; i++) {
            int lead = randomIntBetween(0xF0, 0xF4);
            out[i * 4] = (byte) lead;
            int lo = lead == 0xF0 ? 0x90 : 0x80;
            int hi = lead == 0xF4 ? 0x8F : 0xBF;
            out[i * 4 + 1] = (byte) randomIntBetween(lo, hi);
            out[i * 4 + 2] = (byte) randomIntBetween(0x80, 0xBF);
            out[i * 4 + 3] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
    }

    private byte[] randomValidUnambiguousFourByteRun(int chars) {
        byte[] out = new byte[chars * 4];
        for (int i = 0; i < chars; i++) {
            out[i * 4] = (byte) randomIntBetween(0xF1, 0xF3);
            out[i * 4 + 1] = (byte) randomIntBetween(0x80, 0xBF);
            out[i * 4 + 2] = (byte) randomIntBetween(0x80, 0xBF);
            out[i * 4 + 3] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
    }

    private byte[] randomValidE0Run(int chars) {
        byte[] out = new byte[chars * 3];
        for (int i = 0; i < chars; i++) {
            out[i * 3] = (byte) 0xE0;
            out[i * 3 + 1] = (byte) randomIntBetween(0xA0, 0xBF);
            out[i * 3 + 2] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
    }

    private byte[] randomValidEdRun(int chars) {
        byte[] out = new byte[chars * 3];
        for (int i = 0; i < chars; i++) {
            out[i * 3] = (byte) 0xED;
            out[i * 3 + 1] = (byte) randomIntBetween(0x80, 0x9F);
            out[i * 3 + 2] = (byte) randomIntBetween(0x80, 0xBF);
        }
        return out;
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
