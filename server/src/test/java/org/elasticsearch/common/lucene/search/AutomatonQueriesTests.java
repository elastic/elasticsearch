/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

public class AutomatonQueriesTests extends ESTestCase {

    public void testToCaseInsensitiveChar() {
        int codepoint = randomBoolean() ? randomInt(128) : randomUnicodeOfLength(1).codePointAt(0);
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar(codepoint);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(new String(Character.toChars(codepoint)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        // only codepoints below 128 are converted to a case-insensitive automaton, so only test that for those cases
        if (codepoint <= 128) {
            int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
            br = new BytesRef(new String(Character.toChars(altCase)));
            assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        }
    }

    public void testToCaseInsensitiveString() {
        String s = randomAlphaOfLengthBetween(10, 100);
        Automaton automaton = AutomatonQueries.toCaseInsensitiveString(s);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(s);
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef(randomBoolean() ? s.toLowerCase(Locale.ROOT) : s.toUpperCase(Locale.ROOT));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        // we cannot really upper/lowercase any random unicode string, for details
        // see restrictions in AutomatonQueries.toCaseInsensitiveChar, but we can
        // at least check the original string is accepted
        s = randomRealisticUnicodeOfLengthBetween(10, 100);
        automaton = AutomatonQueries.toCaseInsensitiveString(s);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef(s);
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        s = randomUnicodeOfLengthBetween(10, 100);
        automaton = AutomatonQueries.toCaseInsensitiveString(s);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef(s);
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
    }

    public void testToCaseInsensitivePrefix() {
        String s = randomAlphaOfLengthBetween(10, 100);
        Automaton automaton = AutomatonQueries.caseInsensitivePrefix(s);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(s + randomRealisticUnicodeOfLengthBetween(10, 20));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef(
            (randomBoolean() ? s.toLowerCase(Locale.ROOT) : s.toUpperCase(Locale.ROOT)) + randomRealisticUnicodeOfLengthBetween(10, 20)
        );
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        // We cannot uppercase or lowercase any random unicode string.
        // For details see restrictions in AutomatonQueries.toCaseInsensitiveChar.
        // However, we can at least check the original string is accepted here.
        s = randomRealisticUnicodeOfLengthBetween(10, 100);
        automaton = AutomatonQueries.caseInsensitivePrefix(s);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef(s + randomRealisticUnicodeOfLengthBetween(10, 20));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        s = randomUnicodeOfLengthBetween(10, 100);
        automaton = AutomatonQueries.caseInsensitivePrefix(s);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef(s + randomRealisticUnicodeOfLengthBetween(10, 20));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
    }

    public void testCollapseConsecutiveQuantifierStacksUpToThree() {
        // Generates all +/*/? stacks of length 1..3 and verifies collapse and language equivalence
        // (e.g. a++ -> a+, a+? -> a*, a+*? -> a*).
        char[] quantifiers = new char[] { '+', '*', '?' };
        for (int length = 1; length <= 3; length++) {
            int combinations = (int) Math.pow(quantifiers.length, length);
            for (int i = 0; i < combinations; i++) {
                String stack = toQuantifierStack(i, length, quantifiers);
                char collapsedQuantifier = expectedCollapsedQuantifier(stack);
                assertCollapsedAndLanguagePreserved("a" + stack, "a" + collapsedQuantifier);
            }
        }
    }

    public void testCollapseConsecutiveQuantifiersPathologicalPattern() {
        assertCollapsedAndLanguagePreserved("(ab)+++.+.???.*", "(ab)+.+.?.*");
        assertCollapsedAndLanguagePreserved("(ab)+++++??*", "(ab)*");
        assertCollapsedAndLanguagePreserved("(.[^A-Za-z0-9_])?Ben+++++.?", "(.[^A-Za-z0-9_])?Ben+.?");
    }

    public void testCollapseConsecutiveQuantifiersHandlesEscapes() {
        assertCollapsedAndLanguagePreserved("a\\+\\+\\+", "a\\+\\+\\+");
        assertCollapsedAndLanguagePreserved("a+\\++", "a+\\++");
        assertCollapsedAndLanguagePreserved("\\+\\*\\?", "\\+\\*\\?");
    }

    public void testCollapseConsecutiveQuantifiersHandlesCharClasses() {
        assertCollapsedAndLanguagePreserved("[+*?]+", "[+*?]+");
        assertCollapsedAndLanguagePreserved("[+++]+", "[+++]+");
        assertCollapsedAndLanguagePreserved("[^+*?]++", "[^+*?]+");
    }

    public void testCollapseConsecutiveQuantifiersHandlesQuotedStrings() {
        assertCollapsedAndLanguagePreserved("\"+++\"a+", "\"+++\"a+");
        assertCollapsedAndLanguagePreserved("\"***\"b+", "\"***\"b+");
        assertCollapsedAndLanguagePreserved("a+\"+++\"b+", "a+\"+++\"b+");
    }

    public void testCollapseConsecutiveQuantifiersEmptyAndSimplePatterns() {
        assertCollapsedAndLanguagePreserved("", "");
        assertCollapsedAndLanguagePreserved("abc", "abc");
        assertCollapsedAndLanguagePreserved(".", ".");
    }

    public void testCollapseConsecutiveQuantifiersTrailingBackslash() {
        assertCollapsedAndInvalidRegexHandled("a\\", "a\\");
    }

    public void testCollapseConsecutiveQuantifiersResetsOnNonQuantifier() {
        assertCollapsedAndLanguagePreserved("a++b++", "a+b+");
        assertCollapsedAndLanguagePreserved("a??z**", "a?z*");
    }

    public void testCollapseConsecutiveQuantifiersUnclosedQuoteOrClass() {
        assertCollapsedAndInvalidRegexHandled("\"+++", "\"+++");
        assertCollapsedAndInvalidRegexHandled("[+++", "[+++");
    }

    public void testCollapseConsecutiveQuantifiersNullPattern() {
        expectThrows(NullPointerException.class, () -> AutomatonQueries.collapseConsecutiveQuantifiers(null));
    }

    private static void assertCollapsed(String input, String expected) {
        assertEquals(expected, AutomatonQueries.collapseConsecutiveQuantifiers(input));
    }

    private static void assertCollapsedAndLanguagePreserved(String pattern, String collapsed) {
        assertCollapsed(pattern, collapsed);
        Automaton originalAutomaton = Operations.determinize(new RegExp(pattern).toAutomaton(), 10_000);
        Automaton collapsedAutomaton = Operations.determinize(new RegExp(collapsed).toAutomaton(), 10_000);
        assertTrue(
            "collapsed regex must accept the same language, pattern=[" + pattern + "], collapsed=[" + collapsed + "]",
            AutomatonTestUtil.sameLanguage(originalAutomaton, collapsedAutomaton)
        );
    }

    private void assertCollapsedAndInvalidRegexHandled(String pattern, String expectedCollapsed) {
        assertCollapsed(pattern, expectedCollapsed);
        Exception original = expectThrows(IllegalArgumentException.class, () -> new RegExp(pattern).toAutomaton());
        Exception reduced = expectThrows(IllegalArgumentException.class, () -> new RegExp(expectedCollapsed).toAutomaton());
        assertEquals(original.getMessage(), reduced.getMessage());
    }

    private static String toQuantifierStack(int value, int length, char[] quantifiers) {
        char[] stack = new char[length];
        for (int i = length - 1; i >= 0; i--) {
            stack[i] = quantifiers[value % quantifiers.length];
            value /= quantifiers.length;
        }
        return new String(stack);
    }

    private static char expectedCollapsedQuantifier(String stack) {
        boolean seenPlus = false;
        boolean seenStar = false;
        boolean seenQuestion = false;
        for (char c : stack.toCharArray()) {
            switch (c) {
                case '+' -> seenPlus = true;
                case '*' -> seenStar = true;
                case '?' -> seenQuestion = true;
                default -> throw new IllegalArgumentException("unexpected quantifier [" + c + "]");
            }
        }
        if (seenStar || (seenPlus && seenQuestion)) {
            return '*';
        }
        if (seenPlus) {
            return '+';
        }
        return '?';
    }

    // ------------------------------------------------------------------
    // Pre-flight reservation sizing for the CompiledAutomaton blind window (#147428)
    // ------------------------------------------------------------------

    public void testCompiledAutomatonReservationBytesScalesWithDfa() {
        // Pick inputs that comfortably exceed the floor so we exercise the multiplier path,
        // not the floor.
        long mediumDfa = AutomatonQueries.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES;
        long largeDfa = mediumDfa * 100;
        assertEquals(
            mediumDfa * AutomatonQueries.COMPILED_AUTOMATON_PEAK_MULTIPLIER,
            AutomatonQueries.compiledAutomatonReservationBytes(mediumDfa)
        );
        assertEquals(
            largeDfa * AutomatonQueries.COMPILED_AUTOMATON_PEAK_MULTIPLIER,
            AutomatonQueries.compiledAutomatonReservationBytes(largeDfa)
        );
    }

    public void testCompiledAutomatonReservationBytesAppliesFloorForSmallDfa() {
        // A tiny DFA's K-multiplied size is below the floor; the floor must dominate.
        long tinyDfa = 100L;
        long reservation = AutomatonQueries.compiledAutomatonReservationBytes(tinyDfa);
        assertEquals(AutomatonQueries.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES, reservation);
        assertTrue(
            "reservation must not regress below the floor",
            reservation >= AutomatonQueries.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES
        );
    }

    public void testCompiledAutomatonReservationBytesIsZeroSafe() {
        // Defensive: a 0-byte DFA shouldn't yield a 0 reservation; the floor still applies.
        assertEquals(AutomatonQueries.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES, AutomatonQueries.compiledAutomatonReservationBytes(0));
    }

    /**
     * Verifies that the pre-flight CB reservation covers the post-construction query size for each
     * known pattern family. {@code reservation / actual} ratios are logged so they can be used to
     * tune {@link AutomatonQueries#COMPILED_AUTOMATON_PEAK_MULTIPLIER} and
     * {@link AutomatonQueries#COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES} over time.
     * <p>
     * Note: {@code actual} is the final retained size from {@code query.ramBytesUsed()}, not the
     * construction peak. The reservation must cover the peak; this test only verifies the final
     * state as a lower bound.
     */
    public void testCompiledAutomatonReservationCoversActualSize() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test");
        String[][] patterns = {
            { "simple_ascii", "foo*bar" },
            { "many_wildcards", "a*b?c*d?e*f?g*h" },
            { "multibyte_utf8", "日".repeat(60) + "*" },
            { "adversarial_question", "?".repeat(1000) },
            { "long_literal", "a".repeat(50) + "*" + "b".repeat(50) }, };
        for (String[] entry : patterns) {
            String name = entry[0];
            String pattern = entry[1];
            Term term = new Term("field", pattern);
            Automaton dfa = AutomatonQueries.toWildcardAutomaton(term, breaker);
            long reservation = AutomatonQueries.compiledAutomatonReservationBytes(dfa.ramBytesUsed());
            long actual = new WildcardQuery(term).ramBytesUsed();
            logger.info(
                "CB reservation ratio [{}]: dfa={} B  reservation={} B  actual={} B  ratio={}",
                name,
                dfa.ramBytesUsed(),
                reservation,
                actual,
                String.format(java.util.Locale.ROOT, "%.1f", (double) reservation / actual)
            );
            assertTrue("reservation must cover actual size for pattern [" + name + "]", reservation >= actual);
        }
    }

    public void testCompiledAutomatonReservationBytesSaturatesOnOverflow() {
        // A DFA so large that ramBytes * multiplier would overflow long arithmetic must saturate
        // to Long.MAX_VALUE rather than wrap to a small positive (or negative) value that would
        // silently under-reserve.
        long overflowingDfa = Long.MAX_VALUE / AutomatonQueries.COMPILED_AUTOMATON_PEAK_MULTIPLIER + 1;
        assertEquals(Long.MAX_VALUE, AutomatonQueries.compiledAutomatonReservationBytes(overflowingDfa));
        assertEquals(Long.MAX_VALUE, AutomatonQueries.compiledAutomatonReservationBytes(Long.MAX_VALUE));
    }
}
