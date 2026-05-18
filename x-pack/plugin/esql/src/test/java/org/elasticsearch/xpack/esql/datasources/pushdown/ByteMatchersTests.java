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
}
