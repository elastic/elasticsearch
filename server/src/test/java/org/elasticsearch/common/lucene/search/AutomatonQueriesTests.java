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
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class AutomatonQueriesTests extends ESTestCase {

    public void testCaseInsensitiveCharHandlesAscii() {
        // Verify ASCII case folding works through Lucene's makeCaseInsensitiveChar
        int codepoint = randomInt(128);
        Automaton automaton = Automata.makeCaseInsensitiveChar(codepoint);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(new String(Character.toChars(codepoint)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
        br = new BytesRef(new String(Character.toChars(altCase)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
    }

    public void testCaseInsensitiveCharHandlesUnicode() {
        // Verify Unicode case folding works for non-ASCII codepoints
        int codepoint = randomUnicodeOfLength(1).codePointAt(0);
        Automaton automaton = Automata.makeCaseInsensitiveChar(codepoint);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        // original codepoint is always accepted
        BytesRef br = new BytesRef(new String(Character.toChars(codepoint)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        // uppercase and lowercase variants are accepted
        int upper = Character.toUpperCase(codepoint);
        br = new BytesRef(new String(Character.toChars(upper)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        int lower = Character.toLowerCase(codepoint);
        br = new BytesRef(new String(Character.toChars(lower)));
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
    }

    public void testCaseInsensitiveCharHandlesSpecialUnicodeMappings() {
        // Kelvin sign (U+212A) should match K and k
        Automaton automaton = Automata.makeCaseInsensitiveChar(0x212A);
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef("\u212A");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("K");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("k");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        // K should also match Kelvin sign
        automaton = Automata.makeCaseInsensitiveChar('K');
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef("K");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("k");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("\u212A");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        // Long S (U+017F) should match s and S
        automaton = Automata.makeCaseInsensitiveChar(0x017F);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef("\u017F");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("s");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("S");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));

        // Micro sign (U+00B5) should match Greek small letter mu (U+03BC) and Greek capital letter mu (U+039C)
        automaton = Automata.makeCaseInsensitiveChar(0x00B5);
        runAutomaton = new ByteRunAutomaton(automaton);
        br = new BytesRef("\u00B5");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("\u03BC");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef("\u039C");
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
    }

    public void testMakeCaseInsensitiveString() {
        String s = randomAlphaOfLengthBetween(10, 100);
        Automaton automaton = Automata.makeCaseInsensitiveString(s);
        assertTrue(automaton.isDeterministic());
        ByteRunAutomaton runAutomaton = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(s);
        assertTrue(runAutomaton.run(br.bytes, br.offset, br.length));
        br = new BytesRef(randomBoolean() ? s.toLowerCase(Locale.ROOT) : s.toUpperCase(Locale.ROOT));
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

    public void testToRegexpAutomatonReservesAndReleasesBreakerMemory() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Term term = new Term("field", "a{100}b?c*");
        Automaton dfa = AutomatonQueries.toRegexpAutomaton(term, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, breaker);
        assertTrue(dfa.isDeterministic());
        assertEquals("all reserved memory should be released after a successful build", 0, breaker.getUsed());
    }

    public void testToRegexpAutomatonWithoutBreakerMatchesLucene() {
        Term term = new Term("field", "a{3}b?c*|[x-z]+");
        Automaton dfa = AutomatonQueries.toRegexpAutomaton(term, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null);
        Automaton lucene = Operations.determinize(
            new RegExp(term.text(), RegExp.ALL, 0).toAutomaton(),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        assertTrue(dfa.isDeterministic());
        assertTrue(AutomatonTestUtil.sameLanguage(lucene, dfa));
    }

    /** A repetition past the work limit is refused before anything is reserved, so it cannot trip the breaker. */
    public void testToRegexpAutomatonRefusesHugeRepetitionBeforeReserving() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(500));
        Term term = new Term("field", "a{10000000}");
        expectThrows(
            TooComplexToDeterminizeException.class,
            () -> AutomatonQueries.toRegexpAutomaton(term, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, breaker)
        );
        assertEquals("nothing is reserved for a pattern refused for its work", 0, breaker.getUsed());
        assertEquals("the breaker is not tripped by a pattern refused for its work", 0, breaker.getTrippedCount());
    }

    /** A repetition within the work limit whose predicted peak exceeds the breaker trips it before the build starts. */
    public void testToRegexpAutomatonTripsBreakerForLargeRepetitionBeforeBuild() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        Term term = new Term("field", "a{300000}");
        expectThrows(
            CircuitBreakingException.class,
            () -> AutomatonQueries.toRegexpAutomaton(term, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, breaker)
        );
        assertEquals("no memory should remain reserved after the breaker trips", 0, breaker.getUsed());
    }

    /**
     * Lucene parses nested groups recursively, so a deep pattern overflows the stack while parsing; it must surface as a
     * client error, with and without a breaker.
     */
    public void testDeeplyNestedRegexpIsAClientError() {
        int depth = 20_000;
        Term term = new Term("field", "(".repeat(depth) + "a" + ")".repeat(depth));
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        for (CircuitBreaker maybeBreaker : new CircuitBreaker[] { breaker, null }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AutomatonQueries.toRegexpAutomaton(term, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, maybeBreaker)
            );
            assertThat(e.getMessage(), containsString("too deeply nested"));
        }
        assertEquals(0, breaker.getUsed());
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

}
