/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * {@link ESVectorUtil#mayContainIgnoringAsciiCase} and {@link ESVectorUtil#anyNonAscii}, which a caller reads a
 * value's bytes with to leave out the values that cannot hold a token. The search is allowed to answer more widely
 * than it was asked; it is never allowed to answer more narrowly, so that is what most of this asserts.
 */
public class FoldedContainsTests extends BaseVectorizationTests {

    private static final ESVectorUtilSupport SCALAR = defaultProvider().getVectorUtilSupport();
    private static final ESVectorUtilSupport VECTOR = panamaProvider().getVectorUtilSupport();

    /** Bytes that exercise the fold: the ASCII letters in both cases, the digits, and what sits around them. */
    private static final String ASCII_ALPHABET = "abcXYZ019 .,-_@[^`{~\t\"'/\\";

    public void testATermTheValueSpells() {
        assertTrue(contains("the quick brown fox", "brown"));
        assertTrue("at the first byte", contains("the quick brown fox", "the"));
        assertTrue("at the last byte", contains("the quick brown fox", "fox"));
        assertFalse(contains("the quick brown fox", "bear"));
    }

    public void testCaseIsAnsweredForTheAsciiLetters() {
        assertTrue(contains("The Quick Brown Fox", "brown"));
        assertTrue(contains("the quick brown fox", "BROWN"));
        assertTrue(contains("tHe QuIcK bRoWn", "quick"));
    }

    /** A value holding bytes outside ASCII is searched like any other, which is what a reader of one needs. */
    public void testAValueOutsideAsciiIsSearchedLikeAnyOther() {
        assertFalse("bytes outside ASCII say nothing about an ASCII term", contains("café naïve", "bear"));
        assertTrue(contains("café and a zeppelin", "zeppelin"));
        assertFalse(contains("a value with ‘curly quotes’ and an en–dash", "zeppelin"));
        assertTrue("and a term outside ASCII is found where the value spells it", contains("a café here", "café"));
    }

    public void testATermLongerThanTheValue() {
        assertFalse(contains("fox", "foxes"));
        assertFalse(contains("föx", "foxes"));
    }

    public void testAnEmptyTermIsHeldByEveryValue() {
        assertTrue(contains("", ""));
        assertTrue(contains("anything", ""));
    }

    public void testAnEmptyValueHoldsNoTerm() {
        assertFalse(contains("", "fox"));
    }

    /** The whole point: a value the read rules out holds the term in no case at all. */
    public void testRulingOutAValueNeverRulesOutAMatch() {
        for (int iteration = 0; iteration < 5000; iteration++) {
            final String text = randomAsciiRun(between(0, 120));
            final String token = randomBoolean() ? randomAsciiRun(between(1, 8)) : someStretchOf(text);
            if (contains(text, token) == false) {
                assertFalse(
                    "[" + text + "] was ruled out for [" + token + "] but holds it",
                    text.toLowerCase(Locale.ROOT).contains(token.toLowerCase(Locale.ROOT))
                );
            }
        }
    }

    public void testBothImplementationsAgreeWithTheAnswerSpelledOut() {
        for (int iteration = 0; iteration < 5000; iteration++) {
            final byte[] value = randomBytes(between(0, 140));
            final byte[] term = randomBytes(between(0, 12));
            assertImplementationsAgree(value, 0, value.length, term, 0, term.length);
        }
    }

    /** Every length either side of the 24-byte scalar cut-off and of a vector's own width. */
    public void testEveryLengthAroundTheBoundaries() {
        for (int valueLength = 0; valueLength <= 80; valueLength++) {
            for (int termLength = 0; termLength <= 5; termLength++) {
                final byte[] value = randomBytes(valueLength);
                final byte[] term = randomBytes(termLength);
                assertImplementationsAgree(value, 0, valueLength, term, 0, termLength);
                if (valueLength >= termLength && termLength > 0) {
                    // A term the value does hold, so the walk reaches its confirmation rather than running out.
                    final int at = between(0, valueLength - termLength);
                    System.arraycopy(term, 0, value, at, termLength);
                    assertImplementationsAgree(value, 0, valueLength, term, 0, termLength);
                }
            }
        }
    }

    /**
     * The term, in the other case, at every start of every value length up to 150, so the vector search meets pairs
     * of blocks, the single-block remainder and the overlapping final block with the match in each of them, and
     * the first candidate reported is the one where the term sits. A near miss, with one middle byte replaced,
     * checks that both implementations agree about what is not found.
     */
    public void testTheTermAtEveryPositionOfEveryLength() {
        for (int valueLength = 1; valueLength <= 150; valueLength++) {
            for (int termLength = 1; termLength <= Math.min(valueLength, 40); termLength++) {
                final byte[] term = new byte[termLength];
                final byte[] upper = new byte[termLength];
                for (int k = 0; k < termLength; k++) {
                    term[k] = (byte) ('a' + (k % 26));
                    upper[k] = (byte) ('A' + (k % 26));
                }
                for (int at = 0; at + termLength <= valueLength; at++) {
                    final byte[] value = new byte[valueLength];
                    Arrays.fill(value, (byte) 'x');
                    System.arraycopy(upper, 0, value, at, termLength);
                    assertEquals(
                        "scalar at " + at + " of " + valueLength,
                        at,
                        SCALAR.indexOfCandidateIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength)
                    );
                    assertEquals(
                        "vector at " + at + " of " + valueLength,
                        at,
                        VECTOR.indexOfCandidateIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength)
                    );
                    assertEquals("public", at, ESVectorUtil.indexOfCandidateIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength));
                    assertTrue(VECTOR.mayContainIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength));
                    if (termLength > 2) {
                        value[at + termLength / 2] = (byte) '#';
                        assertEquals(
                            "near miss at " + at + " of " + valueLength,
                            SCALAR.indexOfCandidateIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength),
                            VECTOR.indexOfCandidateIgnoringAsciiCase(value, 0, valueLength, term, 0, termLength)
                        );
                    }
                }
            }
        }
    }

    /** Neither implementation may read outside the range it was given. */
    public void testOnlyTheRangeGivenIsRead() {
        for (int iteration = 0; iteration < 2000; iteration++) {
            final byte[] value = randomBytes(between(1, 160));
            final byte[] term = randomBytes(between(1, 10));
            final int valueOffset = between(0, value.length - 1);
            final int valueLength = between(0, value.length - valueOffset);
            final int termOffset = between(0, term.length - 1);
            final int termLength = between(1, term.length - termOffset);
            assertImplementationsAgree(value, valueOffset, valueLength, term, termOffset, termLength);
        }
    }

    public void testTheTermIsFoundWhereOnlyTheRangeGivenHoldsIt() {
        final byte[] value = "xxxxxxxxxxxxxxxxxxxxxxxxxxFOXxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes(UTF_8);
        final byte[] term = "fox".getBytes(UTF_8);
        assertTrue(ESVectorUtil.mayContainIgnoringAsciiCase(value, 0, value.length, term, 0, term.length));
        assertFalse("the stretch before it does not hold it", ESVectorUtil.mayContainIgnoringAsciiCase(value, 0, 26, term, 0, term.length));
    }

    public void testAByteOutsideAsciiIsFoundWhereverItSits() {
        for (int length = 1; length <= 80; length++) {
            for (int at = 0; at < length; at++) {
                final byte[] value = new byte[length];
                Arrays.fill(value, (byte) 'a');
                assertFalse("all of ASCII", anyNonAscii(value, 0, length));
                value[at] = (byte) 0xC3;
                assertTrue("a byte outside ASCII at " + at + " of " + length, anyNonAscii(value, 0, length));
            }
        }
    }

    public void testOnlyTheRangeGivenIsReadForAByteOutsideAscii() {
        final byte[] value = new byte[64];
        Arrays.fill(value, (byte) 'a');
        value[40] = (byte) 0x80;
        assertTrue(anyNonAscii(value, 0, 64));
        assertFalse("the stretch before it is all ASCII", anyNonAscii(value, 0, 40));
        assertTrue(anyNonAscii(value, 40, 24));
        assertFalse("and the stretch after it", anyNonAscii(value, 41, 23));
    }

    public void testBothImplementationsAgreeOnAByteOutsideAscii() {
        for (int iteration = 0; iteration < 2000; iteration++) {
            final byte[] value = randomBytes(between(0, 140));
            final int offset = value.length == 0 ? 0 : between(0, value.length - 1);
            final int length = between(0, value.length - offset);
            boolean expected = false;
            for (int i = offset; i < offset + length; i++) {
                expected |= value[i] < 0;
            }
            assertEquals("scalar", expected, SCALAR.anyNonAscii(value, offset, length));
            assertEquals("vector", expected, VECTOR.anyNonAscii(value, offset, length));
            assertEquals("public", expected, ESVectorUtil.anyNonAscii(value, offset, length));
        }
    }

    private boolean anyNonAscii(byte[] value, int offset, int length) {
        final boolean scalar = SCALAR.anyNonAscii(value, offset, length);
        assertEquals("the two implementations disagree", scalar, VECTOR.anyNonAscii(value, offset, length));
        return scalar;
    }

    public void testWhereTheTermWasFound() {
        assertEquals(4, indexOf("the quick brown fox", "quick"));
        assertEquals("the first of several", 0, indexOf("the other the", "the"));
        assertEquals("compared without ASCII case", 4, indexOf("the QUICK brown", "quick"));
        assertEquals(-1, indexOf("the quick brown", "bear"));
        assertEquals("an empty term sits at the start", 0, indexOf("the quick", ""));
    }

    /** Searching on from an occurrence walks them all, which is what reading each one asks for. */
    public void testEveryOccurrenceInTurn() {
        final byte[] value = "the cat scattered the cat".getBytes(UTF_8);
        final byte[] term = "cat".getBytes(UTF_8);
        final List<Integer> found = new ArrayList<>();
        for (int from = 0; from <= value.length - term.length;) {
            final int at = ESVectorUtil.indexOfCandidateIgnoringAsciiCase(value, from, value.length - from, term, 0, term.length);
            if (at < 0) {
                break;
            }
            found.add(at);
            from = at + 1;
        }
        assertEquals(List.of(4, 9, 22), found);
    }

    public void testBothImplementationsAgreeOnWhereTheTermWasFound() {
        for (int iteration = 0; iteration < 5000; iteration++) {
            final byte[] value = randomBytes(between(0, 140));
            final byte[] term = randomBytes(between(1, 12));
            final int offset = value.length == 0 ? 0 : between(0, value.length - 1);
            final int length = between(0, value.length - offset);
            if (term.length > length) {
                continue;
            }
            final int expected = indexSpelledOut(value, offset, length, term);
            final String what = "value=" + describe(value, offset, length) + " term=" + describe(term, 0, term.length);
            assertEquals(
                "scalar: " + what,
                expected,
                SCALAR.indexOfCandidateIgnoringAsciiCase(value, offset, length, term, 0, term.length)
            );
            assertEquals(
                "vector: " + what,
                expected,
                VECTOR.indexOfCandidateIgnoringAsciiCase(value, offset, length, term, 0, term.length)
            );
            assertEquals(
                "public: " + what,
                expected,
                ESVectorUtil.indexOfCandidateIgnoringAsciiCase(value, offset, length, term, 0, term.length)
            );
        }
    }

    /** Where the term sits, written out again over the bytes themselves. */
    private static int indexSpelledOut(byte[] value, int valueOffset, int valueLength, byte[] term) {
        for (int at = valueOffset; at <= valueOffset + valueLength - term.length; at++) {
            boolean folds = true;
            for (int i = 0; i < term.length && folds; i++) {
                folds = (byte) (value[at + i] | 0x20) == (byte) (term[i] | 0x20);
            }
            if (folds) {
                return at;
            }
        }
        return -1;
    }

    private static int indexOf(String value, String term) {
        final byte[] valueBytes = value.getBytes(UTF_8);
        final byte[] termBytes = term.getBytes(UTF_8);
        return ESVectorUtil.indexOfCandidateIgnoringAsciiCase(valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length);
    }

    private void assertImplementationsAgree(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength) {
        final boolean expected = spelledOut(value, valueOffset, valueLength, term, termOffset, termLength);
        final String what = "value=" + describe(value, valueOffset, valueLength) + " term=" + describe(term, termOffset, termLength);
        assertEquals("scalar: " + what, expected, call(SCALAR, value, valueOffset, valueLength, term, termOffset, termLength));
        assertEquals("vector: " + what, expected, call(VECTOR, value, valueOffset, valueLength, term, termOffset, termLength));
        assertEquals(
            "public: " + what,
            expected,
            ESVectorUtil.mayContainIgnoringAsciiCase(value, valueOffset, valueLength, term, termOffset, termLength)
        );
    }

    private static boolean call(
        ESVectorUtilSupport impl,
        byte[] value,
        int valueOffset,
        int valueLength,
        byte[] term,
        int termOffset,
        int termLength
    ) {
        if (termLength == 0) {
            return true; // the guard the public entry point makes before any implementation is asked
        }
        if (termLength > valueLength) {
            return false;
        }
        return impl.mayContainIgnoringAsciiCase(value, valueOffset, valueLength, term, termOffset, termLength);
    }

    /** The answer written out again, over strings, so the walk is compared against something that does not walk. */
    private static boolean spelledOut(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength) {
        if (termLength == 0) {
            return true;
        }
        return folded(value, valueOffset, valueLength).contains(folded(term, termOffset, termLength));
    }

    private static String folded(byte[] bytes, int offset, int length) {
        final StringBuilder folded = new StringBuilder(length);
        for (int i = offset; i < offset + length; i++) {
            folded.append((char) ((bytes[i] | 0x20) & 0xFF));
        }
        return folded.toString();
    }

    private static String describe(byte[] bytes, int offset, int length) {
        return "[" + folded(bytes, offset, length) + "]@" + offset + "+" + length;
    }

    private boolean contains(String value, String term) {
        final byte[] valueBytes = value.getBytes(UTF_8);
        final byte[] termBytes = term.getBytes(UTF_8);
        final boolean scalar = call(SCALAR, valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length);
        final boolean vector = call(VECTOR, valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length);
        assertEquals("the two implementations disagree on [" + value + "] for [" + term + "]", scalar, vector);
        return scalar;
    }

    private byte[] randomBytes(int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            // Mostly the bytes the fold is about, with the occasional one outside ASCII.
            bytes[i] = rarely() ? (byte) between(0x80, 0xFF) : (byte) ASCII_ALPHABET.charAt(between(0, ASCII_ALPHABET.length() - 1));
        }
        return bytes;
    }

    private String randomAsciiRun(int length) {
        final StringBuilder run = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            run.append(ASCII_ALPHABET.charAt(between(0, ASCII_ALPHABET.length() - 1)));
        }
        return run.toString();
    }

    private String someStretchOf(String text) {
        if (text.isEmpty()) {
            return "x";
        }
        final int from = between(0, text.length() - 1);
        return text.substring(from, between(from + 1, text.length()));
    }
}
