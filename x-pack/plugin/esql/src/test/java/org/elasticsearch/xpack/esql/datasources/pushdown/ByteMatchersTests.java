/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class ByteMatchersTests extends ESTestCase {

    public void testEqualsHappyPath() {
        assertTrue(ByteMatchers.equals(new BytesRef("hello"), new BytesRef("hello")));
        assertFalse(ByteMatchers.equals(new BytesRef("hello"), new BytesRef("world")));
        // Equal length, single byte differs.
        assertFalse(ByteMatchers.equals(new BytesRef("hellO"), new BytesRef("hello")));
    }

    public void testEqualsLengthMismatchShortCircuits() {
        // Different lengths must return false without descending to Arrays.equals — the
        // pre-check guards the contract that Arrays.equals is only called on equal-length slices.
        assertFalse(ByteMatchers.equals(new BytesRef("hello"), new BytesRef("hello!")));
        assertFalse(ByteMatchers.equals(new BytesRef("hello!"), new BytesRef("hello")));
    }

    public void testEqualsEmpty() {
        assertTrue(ByteMatchers.equals(new BytesRef(""), new BytesRef("")));
        assertFalse(ByteMatchers.equals(new BytesRef(""), new BytesRef("x")));
    }

    public void testEqualsRespectsBytesRefOffset() {
        // BytesRef can hold a slice of a larger backing array — equals must honor offset/length.
        byte[] backing = "XXXhelloYYY".getBytes(StandardCharsets.UTF_8);
        BytesRef sliced = new BytesRef(backing, 3, 5);
        assertTrue(ByteMatchers.equals(sliced, new BytesRef("hello")));
        assertFalse(ByteMatchers.equals(sliced, new BytesRef("XXXhello")));
    }

    public void testStartsWithHappyPath() {
        assertTrue(ByteMatchers.startsWith(new BytesRef("https://www.google.com/"), new BytesRef("https://")));
        assertFalse(ByteMatchers.startsWith(new BytesRef("https://www.google.com/"), new BytesRef("http://")));
    }

    public void testStartsWithEmptyPrefixMatchesAlways() {
        // SQL convention and the wildcard-shape parser both treat an absent prefix as a no-op.
        assertTrue(ByteMatchers.startsWith(new BytesRef("anything"), new BytesRef("")));
        assertTrue(ByteMatchers.startsWith(new BytesRef(""), new BytesRef("")));
    }

    public void testStartsWithLongerPrefixReturnsFalse() {
        assertFalse(ByteMatchers.startsWith(new BytesRef("abc"), new BytesRef("abcd")));
    }

    public void testStartsWithExactMatch() {
        assertTrue(ByteMatchers.startsWith(new BytesRef("abc"), new BytesRef("abc")));
    }

    public void testStartsWithRespectsBytesRefOffset() {
        byte[] backing = "XXXhelloYYY".getBytes(StandardCharsets.UTF_8);
        BytesRef sliced = new BytesRef(backing, 3, 5);
        assertTrue(ByteMatchers.startsWith(sliced, new BytesRef("hel")));
        assertFalse(ByteMatchers.startsWith(sliced, new BytesRef("XXX")));
    }

    public void testEndsWithHappyPath() {
        assertTrue(ByteMatchers.endsWith(new BytesRef("photo.jpg"), new BytesRef(".jpg")));
        assertFalse(ByteMatchers.endsWith(new BytesRef("photo.jpg"), new BytesRef(".png")));
    }

    public void testEndsWithEmptySuffixMatchesAlways() {
        assertTrue(ByteMatchers.endsWith(new BytesRef("anything"), new BytesRef("")));
        assertTrue(ByteMatchers.endsWith(new BytesRef(""), new BytesRef("")));
    }

    public void testEndsWithLongerSuffixReturnsFalse() {
        assertFalse(ByteMatchers.endsWith(new BytesRef("abc"), new BytesRef("zabc")));
    }

    public void testEndsWithExactMatch() {
        assertTrue(ByteMatchers.endsWith(new BytesRef("abc"), new BytesRef("abc")));
    }

    public void testEndsWithRespectsBytesRefOffset() {
        byte[] backing = "XXXhelloYYY".getBytes(StandardCharsets.UTF_8);
        BytesRef sliced = new BytesRef(backing, 3, 5);
        assertTrue(ByteMatchers.endsWith(sliced, new BytesRef("llo")));
        assertFalse(ByteMatchers.endsWith(sliced, new BytesRef("YYY")));
    }

    public void testContainsLiteralHappyPath() {
        // Long enough to clear the SIMD activation threshold (>= 24 bytes inside ESVectorUtil).
        BytesRef url = new BytesRef("https://www.google.com/maps/place/London");
        assertTrue(ByteMatchers.containsLiteral(url, new BytesRef("google")));
        assertTrue(ByteMatchers.containsLiteral(url, new BytesRef("maps")));
        assertFalse(ByteMatchers.containsLiteral(url, new BytesRef("bing")));
    }

    public void testContainsLiteralEmptyMatchesAlways() {
        assertTrue(ByteMatchers.containsLiteral(new BytesRef("anything"), new BytesRef("")));
        assertTrue(ByteMatchers.containsLiteral(new BytesRef(""), new BytesRef("")));
    }

    public void testContainsLiteralLongerThanValueReturnsFalse() {
        assertFalse(ByteMatchers.containsLiteral(new BytesRef("abc"), new BytesRef("abcd")));
    }

    public void testContainsLiteralRespectsLiteralBytesRefOffset() {
        // BinaryDocValuesContainsTermQuery#contains forwards the term's offset; if we ever stop
        // doing so the SIMD scan would search the wrong bytes. Pin the contract from this side.
        byte[] backing = "ZZZgoogleYYY".getBytes(StandardCharsets.UTF_8);
        BytesRef literal = new BytesRef(backing, 3, 6);
        assertTrue(ByteMatchers.containsLiteral(new BytesRef("https://www.google.com"), literal));
        // The 'ZZZ' bytes outside the literal's window must not satisfy the search.
        BytesRef zzzOnly = new BytesRef(backing, 0, 3);
        assertFalse(ByteMatchers.containsLiteral(new BytesRef("https://www.google.com"), zzzOnly));
    }

    public void testAffixContainsExactFitNoLiteral() {
        // prefix.length + suffix.length == value.length, no literal — the affixes meet at the
        // middle with zero bytes between them. Must match.
        assertTrue(ByteMatchers.affixContains(new BytesRef("abcd"), new BytesRef("ab"), null, new BytesRef("cd")));
        // One byte short — combined-length guard rejects.
        assertFalse(ByteMatchers.affixContains(new BytesRef("abc"), new BytesRef("ab"), null, new BytesRef("cd")));
    }

    public void testAffixContainsExactFitWithLiteral() {
        // prefix + literal + suffix == value.length — literal occupies the entire middle slice.
        assertTrue(ByteMatchers.affixContains(new BytesRef("abxxcd"), new BytesRef("ab"), new BytesRef("xx"), new BytesRef("cd")));
        // One byte more than the literal needs — middle slice has room left over, literal still found.
        assertTrue(ByteMatchers.affixContains(new BytesRef("abxxXcd"), new BytesRef("ab"), new BytesRef("xx"), new BytesRef("cd")));
        // Literal does not appear in the middle slice (the only "xx" is straddling prefix/middle).
        assertFalse(ByteMatchers.affixContains(new BytesRef("axxxcd"), new BytesRef("ax"), new BytesRef("xy"), new BytesRef("cd")));
    }

    public void testAffixContainsAllNullBehavesLikeMatchesAll() {
        // Every component absent → match anything non-empty (including empty values).
        assertTrue(ByteMatchers.affixContains(new BytesRef(""), null, null, null));
        assertTrue(ByteMatchers.affixContains(new BytesRef("anything"), null, null, null));
    }

    public void testAffixContainsPrefixOnly() {
        // Equivalent to startsWith.
        assertTrue(ByteMatchers.affixContains(new BytesRef("https://www.google.com/"), new BytesRef("https://"), null, null));
        assertFalse(ByteMatchers.affixContains(new BytesRef("https://www.google.com/"), new BytesRef("http://"), null, null));
    }

    public void testAffixContainsSuffixOnly() {
        assertTrue(ByteMatchers.affixContains(new BytesRef("photo.jpg"), null, null, new BytesRef(".jpg")));
        assertFalse(ByteMatchers.affixContains(new BytesRef("photo.jpg"), null, null, new BytesRef(".png")));
    }

    public void testAffixContainsLiteralOnly() {
        BytesRef url = new BytesRef("https://www.google.com/maps");
        assertTrue(ByteMatchers.affixContains(url, null, new BytesRef("google"), null));
        assertFalse(ByteMatchers.affixContains(url, null, new BytesRef("bing"), null));
    }

    public void testAffixContainsPrefixAndSuffix() {
        // 'http*com' shape — both affixes present, no literal in the middle.
        BytesRef url = new BytesRef("https://www.google.com");
        assertTrue(ByteMatchers.affixContains(url, new BytesRef("http"), null, new BytesRef("com")));
        assertFalse(ByteMatchers.affixContains(url, new BytesRef("ftp"), null, new BytesRef("com")));
        assertFalse(ByteMatchers.affixContains(url, new BytesRef("http"), null, new BytesRef("net")));
    }

    public void testAffixContainsAllThreeComponents() {
        // 'http*google*com' shape — all three present.
        BytesRef url = new BytesRef("https://www.google.com");
        assertTrue(ByteMatchers.affixContains(url, new BytesRef("http"), new BytesRef("google"), new BytesRef("com")));
        // Same value with a literal that is not present in the middle slice.
        assertFalse(ByteMatchers.affixContains(url, new BytesRef("http"), new BytesRef("yahoo"), new BytesRef("com")));
        // Affixes don't fit.
        assertFalse(ByteMatchers.affixContains(new BytesRef("ab"), new BytesRef("a"), new BytesRef("x"), new BytesRef("b")));
    }

    public void testAffixContainsLiteralMustLiveStrictlyBetweenAffixes() {
        // Pins the contract that the literal scan is restricted to value[prefix.length .. value.length - suffix.length].
        // Without that restriction, the literal "aXa" would match the *full* value "aXa" trivially —
        // but the affix-contains shape "a*aXa*a" semantically requires "aXa" to appear between the
        // a-prefix and a-suffix, i.e. inside an empty middle slice. That cannot match.
        BytesRef value = new BytesRef("aXa");
        assertFalse(ByteMatchers.affixContains(value, new BytesRef("a"), new BytesRef("aXa"), new BytesRef("a")));
        // But "a*X*a" does match — middle slice is "X" and "X" appears in it.
        assertTrue(ByteMatchers.affixContains(value, new BytesRef("a"), new BytesRef("X"), new BytesRef("a")));
    }

    public void testAffixContainsRespectsBytesRefOffset() {
        // Backing array carries garbage on either side of the value; offsets must isolate the
        // affix and middle-slice scans to the value's window.
        byte[] backing = "ZZZhttps://www.google.com/mapsZZZ".getBytes(StandardCharsets.UTF_8);
        BytesRef value = new BytesRef(backing, 3, 27);
        assertTrue(ByteMatchers.affixContains(value, new BytesRef("https://"), new BytesRef("google"), new BytesRef("/maps")));
        // The 'ZZZ' bytes outside the window must not satisfy the affixes.
        assertFalse(ByteMatchers.affixContains(value, new BytesRef("ZZZ"), null, null));
        assertFalse(ByteMatchers.affixContains(value, null, null, new BytesRef("ZZZ")));
    }

    public void testAffixContainsCombinedLengthGuard() {
        // prefix + literal + suffix together exceed value length → false without further work.
        // Without the guard, this would either OOB or do unnecessary scans.
        BytesRef shortValue = new BytesRef("abc");
        assertFalse(ByteMatchers.affixContains(shortValue, new BytesRef("ab"), new BytesRef("xyz"), new BytesRef("c")));
    }

    public void testContainsLiteralAcrossSimdBoundary() {
        // ESVectorUtil#contains activates the SIMD first+last-byte filter at value length >= 24.
        // Walk a representative set of sizes straddling and well past that boundary to catch any
        // off-by-one between the scalar and vector code paths.
        for (int size : new int[] { 1, 23, 24, 25, 31, 32, 33, 47, 48, 49, 63, 64, 65, 127, 128, 1024 }) {
            byte[] buf = new byte[size];
            for (int i = 0; i < size; i++) {
                buf[i] = (byte) ('a' + (i % 26));
            }
            BytesRef value = new BytesRef(buf);
            // Literal at the very start.
            assertTrue("hit at start, size=" + size, ByteMatchers.containsLiteral(value, new BytesRef(new byte[] { buf[0] })));
            // Literal in the middle (5 bytes wide where the value is long enough).
            if (size >= 5) {
                int mid = size / 2 - 2;
                byte[] lit = new byte[Math.min(5, size)];
                System.arraycopy(buf, mid, lit, 0, lit.length);
                assertTrue("hit at middle, size=" + size, ByteMatchers.containsLiteral(value, new BytesRef(lit)));
            }
            // Literal at the very end.
            byte[] tail = new byte[] { buf[size - 1] };
            assertTrue("hit at end, size=" + size, ByteMatchers.containsLiteral(value, new BytesRef(tail)));
            // Single-byte literal that is guaranteed absent (any byte in the value buffer is in
            // [a..z]; '!' is not).
            assertFalse("miss, size=" + size, ByteMatchers.containsLiteral(value, new BytesRef("!")));
        }
    }

    public void testContainsLiteralMultibyteUtf8() {
        // UTF-8 is self-synchronizing so byte-level contains is codepoint-correct for valid UTF-8.
        // Pin that contract here.
        BytesRef value = new BytesRef("voilà café naïve résumé Москва 北京 東京");
        assertTrue(ByteMatchers.containsLiteral(value, new BytesRef("café")));
        assertTrue(ByteMatchers.containsLiteral(value, new BytesRef("Москва")));
        assertTrue(ByteMatchers.containsLiteral(value, new BytesRef("北京")));
        assertFalse(ByteMatchers.containsLiteral(value, new BytesRef("paris")));
    }

    public void testContainsLiteralRandomizedAgainstStringIndexOf() {
        // Cross-check the byte-level SIMD contains against the JDK's String#contains on randomized
        // inputs. The inputs are restricted to ASCII so the two semantics coincide and we are only
        // testing the search, not encoding edge cases.
        for (int iter = 0; iter < 2000; iter++) {
            int valueLen = randomIntBetween(0, 128);
            int literalLen = randomIntBetween(0, Math.min(valueLen + 2, 16));
            String value = randomAlphaOfLength(valueLen);
            String literal = literalLen == 0 ? "" : randomAlphaOfLength(literalLen);
            // Bias half the iterations to "hit" cases by splicing the literal into the value.
            if (literalLen > 0 && literalLen <= valueLen && randomBoolean()) {
                int insertAt = randomIntBetween(0, valueLen - literalLen);
                value = value.substring(0, insertAt) + literal + value.substring(insertAt + literalLen);
            }
            boolean expected = value.contains(literal);
            boolean actual = ByteMatchers.containsLiteral(new BytesRef(value), new BytesRef(literal));
            assertEquals("value=[" + value + "] literal=[" + literal + "]", expected, actual);
        }
    }
}
