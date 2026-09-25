/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * End-to-end tests for the {@code String} {@code @allocates} estimators (concat, substring, toCharArray, case mapping,
 * trim): each charges its result's byte cost, computed from the receiver/argument lengths, before the allocating call runs.
 * String literals in the scripts are constant-pool loads and are not charged, so the observed total is the method's alone.
 */
public class AllocationStringEstimatorTests extends AllocationTestCase {

    public void testFormatCharged() {
        // The def[] holding the arguments is charged too, so measure against a script that builds it and stops there.
        long args = allocatedBytes("def[] a = new def[] {\"a\", \"b\"}; return \"x\";");
        long withFormat = allocatedBytes("def[] a = new def[] {\"a\", \"b\"}; String.format(\"%s-%s\", a); return \"x\";");

        assertEquals(AllocationEstimators.formatBytes("%s-%s", new Object[] { "a", "b" }), withFormat - args);
    }

    public void testFormatWithLocaleCharged() {
        long args = allocatedBytes("def[] a = new def[] {\"a\"}; return \"x\";");
        long withFormat = allocatedBytes("def[] a = new def[] {\"a\"}; String.format(Locale.ROOT, \"%s\", a); return \"x\";");

        assertEquals(AllocationEstimators.formatBytes(Locale.ROOT, "%s", new Object[] { "a" }), withFormat - args);
    }

    public void testJoinSizesFromCollectionWithoutConsumingIt() {
        // An estimator must not iterate its argument, so the count comes from Collection.size().
        String build = "List l = new ArrayList(); l.add(\"a\"); l.add(\"b\"); l.add(\"c\"); ";
        long list = allocatedBytes(build + "return \"x\";");
        long withJoin = allocatedBytes(build + "String.join(\",\", l); return \"x\";");

        assertEquals(AllocationEstimators.joinBytes(",", List.of("a", "b", "c")), withJoin - list);
    }

    public void testStringBuilderFromCharSequenceCharged() {
        assertEquals(
            AllocationEstimators.stringBuilderBytes("hello world"),
            allocatedBytes("String s = \"hello world\"; new StringBuilder(s); return \"x\";")
        );
    }

    public void testStringBufferFromCharSequenceCharged() {
        assertEquals(
            AllocationEstimators.stringBufferBytes("hello world"),
            allocatedBytes("String s = \"hello world\"; new StringBuffer(s); return \"x\";")
        );
    }

    public void testStringBuilderSubstringCharged() {
        long builder = allocatedBytes("StringBuilder b = new StringBuilder(); b.append(\"hello\"); return \"x\";");
        long withSubstring = allocatedBytes("StringBuilder b = new StringBuilder(); b.append(\"hello\"); b.substring(2); return \"x\";");

        assertEquals(AllocationEstimators.substringBytes(new StringBuilder("hello"), 2), withSubstring - builder);
        assertThat(withSubstring - builder, greaterThan(0L));
    }

    public void testStringBufferSubstringCharged() {
        long buffer = allocatedBytes("StringBuffer b = new StringBuffer(); b.append(\"hello\"); return \"x\";");
        long withSubstring = allocatedBytes("StringBuffer b = new StringBuffer(); b.append(\"hello\"); b.substring(2); return \"x\";");

        assertEquals(AllocationEstimators.substringBytes(new StringBuffer("hello"), 2), withSubstring - buffer);
        assertThat(withSubstring - buffer, greaterThan(0L));
    }

    public void testStringBuilderSubstringRangeCharged() {
        long builder = allocatedBytes("StringBuilder b = new StringBuilder(); b.append(\"hello\"); return \"x\";");
        long withSubstring = allocatedBytes("StringBuilder b = new StringBuilder(); b.append(\"hello\"); b.substring(1, 4); return \"x\";");

        assertEquals(AllocationEstimators.substringBytes(new StringBuilder(), 1, 4), withSubstring - builder);
    }

    public void testStringBufferSubstringRangeCharged() {
        long buffer = allocatedBytes("StringBuffer b = new StringBuffer(); b.append(\"hello\"); return \"x\";");
        long withSubstring = allocatedBytes("StringBuffer b = new StringBuffer(); b.append(\"hello\"); b.substring(1, 4); return \"x\";");

        assertEquals(AllocationEstimators.substringBytes(new StringBuffer(), 1, 4), withSubstring - buffer);
    }

    public void testConcatCharged() {
        assertEquals(
            AllocationEstimators.concatBytes("hello", "world"),
            allocatedBytes("String s = \"hello\"; s.concat(\"world\"); return \"x\";")
        );
    }

    public void testSubstringFromCharged() {
        assertEquals(
            AllocationEstimators.substringBytes("hello world", 6),
            allocatedBytes("String s = \"hello world\"; s.substring(6); return \"x\";")
        );
    }

    public void testToCharArrayCharged() {
        assertEquals(
            AllocationEstimators.toCharArrayBytes("hello"),
            allocatedBytes("String s = \"hello\"; s.toCharArray(); return \"x\";")
        );
    }

    public void testToLowerCaseCharged() {
        assertEquals(AllocationEstimators.recaseBytes("HELLO"), allocatedBytes("String s = \"HELLO\"; s.toLowerCase(); return \"x\";"));
    }

    public void testToUpperCaseLocaleCharged() {
        // Exercises the (String, Locale) estimator overload resolving against the toUpperCase(Locale) signature.
        assertEquals(
            AllocationEstimators.recaseBytes("hello", Locale.ROOT),
            allocatedBytes("String s = \"hello\"; s.toUpperCase(Locale.ROOT); return \"x\";")
        );
    }

    public void testTrimCharged() {
        assertEquals(AllocationEstimators.recaseBytes("  hi  "), allocatedBytes("String s = \"  hi  \"; s.trim(); return \"x\";"));
    }

    public void testConcatTripsLimit() {
        assertTripsLimit("String s = \"hello\"; s.concat(\"world\"); return \"x\";");
    }

    public void testToCharArrayTripsLimit() {
        assertTripsLimit("String s = \"hello\"; s.toCharArray(); return \"x\";");
    }

    // ---- the rest of java.lang ----

    public void testSubSequenceCharged() {
        assertEquals(
            AllocationEstimators.subSequenceBytes("hello world", 0, 5),
            allocatedBytes("CharSequence c = \"hello world\"; c.subSequence(0, 5); return \"x\";")
        );
    }

    public void testBuilderToStringChargedFromItsLength() {
        StringBuilder hello = new StringBuilder("hello");
        assertEquals(
            AllocationEstimators.stringBuilderBytes("hello") + AllocationEstimators.toStringBytes(hello),
            allocatedBytes("StringBuilder sb = new StringBuilder(\"hello\"); sb.toString(); return \"x\";")
        );
    }

    public void testStringToStringChargesNothing() {
        // A String's toString and String.valueOf return the String itself, typed or through def.
        assertEquals(0L, AllocationEstimators.toStringBytes((Object) "abc"));
        assertEquals(0L, AllocationEstimators.stringValueOfBytes("abc"));
        assertEquals(0L, allocatedBytes("String s = \"abc\"; s.toString(); return \"x\";"));
        assertEquals(0L, allocatedBytes("def s = \"abc\"; s.toString(); return \"x\";"));
        assertEquals(0L, allocatedBytes("String.valueOf(\"abc\"); return \"x\";"));
    }

    public void testObjectToStringChargedAnAllowance() {
        // A map's toString gets the concat allowance plus the String object.
        long map = AllocationEstimators.toStringBytes((Object) java.util.Map.of());
        assertEquals(AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + (long) AllocSizes.NON_STRING_OBJECT_CONCAT_BYTES, map);
        assertEquals(64L + map, allocatedBytes("Map m = new HashMap(); m.toString(); return \"x\";"));
    }

    public void testValueOfNumberChargedExactly() {
        // The int is boxed, then counted as one char without rendering it.
        long one = AllocationEstimators.stringValueOfBytes(5);
        assertEquals(AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + 2L, one);
        assertEquals(AllocSizes.boxSize(int.class) + one, allocatedBytes("String.valueOf(5); return \"x\";"));
    }

    public void testRenderedCharsCountsWithoutRendering() {
        assertEquals(1L, AllocSizes.decimalChars(0));
        assertEquals(4L, AllocSizes.decimalChars(-123));
        assertEquals(String.valueOf(Long.MIN_VALUE).length(), AllocSizes.decimalChars(Long.MIN_VALUE));
        assertEquals(String.valueOf(Long.MAX_VALUE).length(), AllocSizes.decimalChars(Long.MAX_VALUE));
        assertEquals(4L, AllocSizes.renderedChars(null));
        assertEquals(3L, AllocSizes.renderedChars("abc"));
        assertEquals(5L, AllocSizes.renderedChars(true));
        assertEquals(1L, AllocSizes.renderedChars('c'));
        assertThat(AllocSizes.renderedChars(-Double.MAX_VALUE), greaterThanOrEqualTo((long) String.valueOf(-Double.MAX_VALUE).length()));
        assertThat(AllocSizes.renderedChars(-Float.MAX_VALUE), greaterThanOrEqualTo((long) String.valueOf(-Float.MAX_VALUE).length()));
        java.math.BigInteger big = java.math.BigInteger.TEN.pow(100);
        assertThat(AllocSizes.renderedChars(big), greaterThanOrEqualTo((long) big.toString().length()));
        assertEquals(AllocSizes.NON_STRING_OBJECT_CONCAT_BYTES / 2L, AllocSizes.renderedChars(List.of()));
    }

    public void testCopyValueOfCharged() {
        long array = AllocSizes.arraySize(char.class, 2);
        assertEquals(
            array + AllocationEstimators.copyValueOfBytes(new char[2]),
            allocatedBytes("char[] c = new char[2]; String.copyValueOf(c); return \"x\";")
        );
        assertEquals(
            array + AllocationEstimators.copyValueOfBytes(new char[2], 1, 1),
            allocatedBytes("char[] c = new char[2]; String.copyValueOf(c, 1, 1); return \"x\";")
        );
    }

    public void testBase64Charged() {
        assertEquals(AllocationEstimators.encodeBase64Bytes("hello"), allocatedBytes("'hello'.encodeBase64(); return \"x\";"));
        assertEquals(AllocationEstimators.decodeBase64Bytes("aGVsbG8="), allocatedBytes("'aGVsbG8='.decodeBase64(); return \"x\";"));
        assertThat(AllocationEstimators.encodeBase64Bytes("hello"), greaterThan(AllocationEstimators.decodeBase64Bytes("hello")));
    }

    public void testSplitOnTokenChargedAsWorstCase() {
        assertEquals(AllocationEstimators.splitOnTokenBytes("a,b,c", ","), allocatedBytes("'a,b,c'.splitOnToken(','); return \"x\";"));
        assertEquals(
            AllocationEstimators.splitOnTokenBytes("a,b,c", ",", 2),
            allocatedBytes("'a,b,c'.splitOnToken(',', 2); return \"x\";")
        );
        assertThat(AllocationEstimators.splitOnTokenBytes("a,b,c", ",", 2), lessThan(AllocationEstimators.splitOnTokenBytes("a,b,c", ",")));
        // A limit under one means no limit.
        assertEquals(AllocationEstimators.splitOnTokenBytes("a,b,c", ","), AllocationEstimators.splitOnTokenBytes("a,b,c", ",", -1));
    }

    public void testReplaceBoundGrowsWithTheReplacement() {
        long grow = AllocationEstimators.replaceBytes(null, "aXbXc", "X", "YY");
        assertEquals(grow, allocatedBytes("'aXbXc'.replace('X', 'YY'); return \"x\";"));
        assertThat(grow, greaterThan(AllocationEstimators.replaceBytes(null, "aXbXc", "X", "Y")));
        // An empty target matches before and after every char.
        assertThat(
            AllocationEstimators.replaceBytes(null, "abc", "", "-"),
            greaterThan(AllocationEstimators.replaceBytes(null, "abc", "b", "-"))
        );
    }

    public void testReplaceWithFunctionCharged() {
        // The lambda's capture object is charged; the regex literal is a constant and is not.
        long lambda = AllocSizes.captureSize(1);
        assertEquals(
            lambda + AllocationEstimators.replaceAllBytes(null, "abc", 0, Pattern.compile("b"), null),
            allocatedBytes("'abc'.replaceAll(/b/, m -> 'x'); return \"x\";")
        );
        assertEquals(
            lambda + AllocationEstimators.replaceFirstBytes("abc", 0, Pattern.compile("b"), null),
            allocatedBytes("'abc'.replaceFirst(/b/, m -> 'x'); return \"x\";")
        );
    }

    public void testCharacterMembersCharged() {
        assertEquals(AllocationEstimators.characterNameBytes(65), allocatedBytes("Character.getName(65); return \"x\";"));
        assertEquals(AllocationEstimators.forNameBytes("LATIN"), allocatedBytes("Character.UnicodeScript.forName('LATIN'); return \"x\";"));
        assertEquals(AllocationEstimators.unicodeScriptValuesBytes(), allocatedBytes("Character.UnicodeScript.values(); return \"x\";"));
        assertThat(AllocationEstimators.unicodeScriptValuesBytes(), greaterThan(1000L));
    }

    public void testStackTraceElementCharged() {
        assertEquals(
            AllocationEstimators.stackTraceElementBytes(null, null, null, 0),
            allocatedBytes("new StackTraceElement('a', 'b', 'c', 1); return \"x\";")
        );
    }

    public void testToStringRunawayTripsLimit() {
        // Every toString copies the builder, and each copy is charged.
        assertTripsLimit(
            "StringBuilder sb = new StringBuilder(); sb.setLength(10000); for (int i = 0; i < 1000; ++i) { sb.toString(); } return \"x\";",
            "1mb"
        );
    }

    // ---- Pattern.split, an @inject_constant augmentation ----

    public void testPatternSplitCharged() {
        assertEquals(
            AllocationEstimators.patternSplitBytes(Pattern.compile(","), 0, "a,b"),
            allocatedBytes("/,/.split('a,b'); return \"x\";")
        );
    }

    public void testPatternSplitWithLimitCapsPieces() {
        long capped = AllocationEstimators.patternSplitBytes(Pattern.compile(","), 0, "a,b,c", 2);
        assertEquals(capped, allocatedBytes("/,/.split('a,b,c', 2); return \"x\";"));
        assertThat(capped, lessThan(AllocationEstimators.patternSplitBytes(Pattern.compile(","), 0, "a,b,c")));
    }

    public void testPatternSplitChargedThroughDef() {
        // An @inject_constant augmentation with @allocates. The def path has to pass the injected limit to the estimator too.
        assertEquals(
            AllocationEstimators.patternSplitBytes(Pattern.compile(","), 0, "a,b"),
            allocatedBytes("def p = /,/; p.split('a,b'); return \"x\";")
        );
    }

    public void testPatternSplitTripsLimit() {
        assertTripsLimit("/,/.split('a,b'); return \"x\";");
    }
}
