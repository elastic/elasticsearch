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
        long builder = allocatedBytes("StringBuilder b = new StringBuilder(); return \"x\";");
        long withSubstring = allocatedBytes("StringBuilder b = new StringBuilder(); b.substring(0); return \"x\";");

        assertEquals(AllocationEstimators.substringBytes(new StringBuilder(), 0), withSubstring - builder);
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
}
