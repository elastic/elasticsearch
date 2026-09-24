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

    // ---- StringBuilder and StringBuffer grow only when an append does not fit ----

    public void testAppendWithinCapacityChargesNothing() {
        assertEquals(0L, AllocationEstimators.appendBytes(new StringBuilder(), "abc"));
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes(),
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.append(\"abc\"); return \"x\";")
        );
    }

    public void testAppendPastCapacityChargesTheNewArray() {
        // 19 chars into a 16-char builder: the JDK grows to twice the capacity plus two, 34 chars.
        String text = "0123456789abcdefXYZ";
        assertEquals(AllocSizes.arrayBytes(34, 2), AllocationEstimators.appendBytes(new StringBuilder(), text));
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocationEstimators.appendBytes(new StringBuilder(), text),
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.append(\"0123456789abcdefXYZ\"); return \"x\";")
        );
    }

    public void testAppendNumberChargesItsExactString() {
        // The int is boxed on the way in, then the builder makes a one-char String of it. Nothing grows, and the number's
        // length is measured rather than guessed so a loop of appends is not charged growth it never does.
        long one = AllocationEstimators.appendBytes(new StringBuilder(), 5);
        assertEquals(AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + 2L, one);
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocSizes.boxSize(int.class) + one,
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.append(5); return \"x\";")
        );
    }

    public void testAppendOtherObjectChargesAnAllowance() {
        // An arbitrary object's toString length is unknowable, so it gets the same allowance as a concat operand.
        long list = AllocationEstimators.appendBytes(new StringBuilder(), List.of());
        assertThat(list, greaterThan((long) AllocSizes.NON_STRING_OBJECT_CONCAT_BYTES));
    }

    public void testSetLengthGrowthCharged() {
        assertEquals(AllocSizes.arrayBytes(100, 2), AllocationEstimators.setLengthBytes(new StringBuilder(), 100));
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocationEstimators.setLengthBytes(new StringBuilder(), 100),
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.setLength(100); return \"x\";")
        );
    }

    public void testAppendCodePointAtCapacityGrows() {
        // setLength(16) fills the default builder exactly; the code point then needs room for two more chars.
        StringBuilder full = new StringBuilder();
        full.setLength(16);
        assertEquals(0L, AllocationEstimators.setLengthBytes(new StringBuilder(), 16));
        assertEquals(AllocSizes.arrayBytes(34, 2), AllocationEstimators.appendCodePointBytes(full, 65));
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocationEstimators.appendCodePointBytes(full, 65),
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.setLength(16); sb.appendCodePoint(65); return \"x\";")
        );
    }

    public void testReplaceAndInsertGrowthCharged() {
        StringBuilder abc = new StringBuilder("abc");
        String text = "0123456789abcdefXYZ";
        long replace = AllocationEstimators.replaceBytes(abc, 0, 1, text);
        long insert = AllocationEstimators.insertBytes(new StringBuilder(), 0, text);
        assertThat(replace, greaterThan(0L));
        assertThat(insert, greaterThan(0L));
        assertEquals(
            AllocationEstimators.stringBuilderBytes("abc") + replace,
            allocatedBytes("StringBuilder sb = new StringBuilder(\"abc\"); sb.replace(0, 1, \"0123456789abcdefXYZ\"); return \"x\";")
        );
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + insert,
            allocatedBytes("StringBuilder sb = new StringBuilder(); sb.insert(0, \"0123456789abcdefXYZ\"); return \"x\";")
        );
    }

    public void testStringBufferAppendGrowthCharged() {
        String text = "0123456789abcdefXYZ";
        assertEquals(
            AllocationEstimators.stringBufferShellBytes() + AllocationEstimators.appendBytes(new StringBuffer(), text),
            allocatedBytes("StringBuffer sb = new StringBuffer(); sb.append(\"0123456789abcdefXYZ\"); return \"x\";")
        );
    }

    public void testAppendableAppendCharged() {
        // Through the Appendable type the builder still reports its own growth.
        String text = "abcdefghijklmnopqrstuvwxyz";
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocationEstimators.appendableAppendBytes(new StringBuilder(), text, 0, 26),
            allocatedBytes("Appendable a = new StringBuilder(); a.append(\"abcdefghijklmnopqrstuvwxyz\", 0, 26); return \"x\";")
        );
    }

    public void testAppendChargedThroughDef() {
        String text = "0123456789abcdefXYZ";
        assertEquals(
            AllocationEstimators.stringBuilderShellBytes() + AllocationEstimators.appendBytes(new StringBuilder(), text),
            allocatedBytes("def sb = new StringBuilder(); sb.append(\"0123456789abcdefXYZ\"); return \"x\";")
        );
    }
}
