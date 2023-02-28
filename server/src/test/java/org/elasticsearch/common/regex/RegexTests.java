/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class RegexTests extends ESTestCase {
    public void testFlags() {
        String[] supportedFlags = new String[] {
            "CASE_INSENSITIVE",
            "MULTILINE",
            "DOTALL",
            "UNICODE_CASE",
            "CANON_EQ",
            "UNIX_LINES",
            "LITERAL",
            "COMMENTS",
            "UNICODE_CHAR_CLASS",
            "UNICODE_CHARACTER_CLASS" };
        int[] flags = new int[] {
            Pattern.CASE_INSENSITIVE,
            Pattern.MULTILINE,
            Pattern.DOTALL,
            Pattern.UNICODE_CASE,
            Pattern.CANON_EQ,
            Pattern.UNIX_LINES,
            Pattern.LITERAL,
            Pattern.COMMENTS,
            Regex.UNICODE_CHARACTER_CLASS };
        Random random = random();
        int num = 10 + random.nextInt(100);
        for (int i = 0; i < num; i++) {
            int numFlags = random.nextInt(flags.length + 1);
            int current = 0;
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < numFlags; j++) {
                int index = random.nextInt(flags.length);
                current |= flags[index];
                builder.append(supportedFlags[index]);
                if (j < numFlags - 1) {
                    builder.append("|");
                }
            }
            String flagsToString = Regex.flagsToString(current);
            assertThat(Regex.flagsFromString(builder.toString()), equalTo(current));
            assertThat(Regex.flagsFromString(builder.toString()), equalTo(Regex.flagsFromString(flagsToString)));
            Pattern.compile("\\w\\d{1,2}", current); // accepts the flags?
        }
    }

    public void testDoubleWildcardMatch() {
        assertTrue(Regex.simpleMatch("ddd", "ddd"));
        assertTrue(Regex.simpleMatch("ddd", "Ddd", true));
        assertFalse(Regex.simpleMatch("ddd", "Ddd"));
        assertTrue(Regex.simpleMatch("d*d*d", "dadd"));
        assertTrue(Regex.simpleMatch("**ddd", "dddd"));
        assertTrue(Regex.simpleMatch("**ddD", "dddd", true));
        assertFalse(Regex.simpleMatch("**ddd", "fff"));
        assertTrue(Regex.simpleMatch("fff*ddd", "fffabcddd"));
        assertTrue(Regex.simpleMatch("fff**ddd", "fffabcddd"));
        assertFalse(Regex.simpleMatch("fff**ddd", "fffabcdd"));
        assertTrue(Regex.simpleMatch("fff*******ddd", "fffabcddd"));
        assertTrue(Regex.simpleMatch("fff*******ddd", "FffAbcdDd", true));
        assertFalse(Regex.simpleMatch("fff******ddd", "fffabcdd"));
    }

    public void testSimpleMatch() {
        for (int i = 0; i < 1000; i++) {
            final String matchingString = randomAlphaOfLength(between(0, 50));

            // construct a pattern that matches this string by repeatedly replacing random substrings with '*' characters
            String pattern = matchingString;
            for (int shrink = between(0, 5); shrink > 0; shrink--) {
                final int shrinkStart = between(0, pattern.length());
                final int shrinkEnd = between(shrinkStart, pattern.length());
                pattern = pattern.substring(0, shrinkStart) + "*" + pattern.substring(shrinkEnd);
            }
            assertTrue("[" + pattern + "] should match [" + matchingString + "]", Regex.simpleMatch(pattern, matchingString));
            assertTrue(
                "[" + pattern + "] should match [" + matchingString.toUpperCase(Locale.ROOT) + "]",
                Regex.simpleMatch(pattern, matchingString.toUpperCase(Locale.ROOT), true)
            );

            // construct a pattern that does not match this string by inserting a non-matching character (a digit)
            final int insertPos = between(0, pattern.length());
            pattern = pattern.substring(0, insertPos) + between(0, 9) + pattern.substring(insertPos);
            assertFalse("[" + pattern + "] should not match [" + matchingString + "]", Regex.simpleMatch(pattern, matchingString));
        }
    }

    public void testSimpleMatchToAutomaton() {
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("ddd")).run("ddd"));
        assertFalse(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("ddd")).run("Ddd"));
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("d*d*d")).run("dadd"));
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("**ddd")).run("dddd"));
        assertFalse(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("**ddd")).run("fff"));
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("fff*ddd")).run("fffabcddd"));
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("fff**ddd")).run("fffabcddd"));
        assertFalse(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("fff**ddd")).run("fffabcdd"));
        assertTrue(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("fff*******ddd")).run("fffabcddd"));
        assertFalse(new CharacterRunAutomaton(Regex.simpleMatchToAutomaton("fff******ddd")).run("fffabcdd"));
    }

    public void testSimpleMatchManyToAutomaton() {
        assertMatchesAll(Regex.simpleMatchToAutomaton("ddd", "fff"), "ddd", "fff");
        assertMatchesNone(Regex.simpleMatchToAutomaton("ddd", "fff"), "Ddd", "Fff");
        assertMatchesAll(Regex.simpleMatchToAutomaton("d*d*d", "cc"), "dadd", "cc");
        assertMatchesNone(Regex.simpleMatchToAutomaton("d*d*d", "cc"), "dadc", "Cc");
    }

    public void testThousands() throws IOException {
        String[] patterns = new String[10000];
        for (int i = 0; i < patterns.length; i++) {
            patterns[i] = Integer.toString(i, Character.MAX_RADIX);
        }
        Automaton automaton = Regex.simpleMatchToAutomaton(patterns);
        CharacterRunAutomaton run = new CharacterRunAutomaton(automaton);
        for (String pattern : patterns) {
            assertTrue("matches " + pattern, run.run(pattern));
        }
        for (int i = 0; i < 100000; i++) {
            int idx = between(0, Integer.MAX_VALUE);
            String pattern = Integer.toString(idx, Character.MAX_RADIX);
            if (idx < patterns.length) {
                assertTrue("matches " + pattern, run.run(pattern));
            } else {
                assertFalse("matches " + pattern, run.run(pattern));
            }
        }
    }

    public void testThousandsAndPattern() throws IOException {
        int patternCount = 10000;
        String[] patterns = new String[patternCount + 2];
        for (int i = 0; i < patternCount; i++) {
            patterns[i] = Integer.toString(i, Character.MAX_RADIX);
        }
        patterns[patternCount] = "foo*bar";
        patterns[patternCount + 1] = "baz*bort";
        Automaton automaton = Regex.simpleMatchToAutomaton(patterns);
        CharacterRunAutomaton run = new CharacterRunAutomaton(automaton);
        for (int i = 0; i < patternCount; i++) {
            assertTrue("matches " + patterns[i], run.run(patterns[i]));
        }
        assertTrue("matches foobar", run.run("foobar"));
        assertTrue("matches foostuffbar", run.run("foostuffbar"));
        assertTrue("matches bazbort", run.run("bazbort"));
        assertTrue("matches bazstuffbort", run.run("bazstuffbort"));
        for (int i = 0; i < 100000; i++) {
            int idx = between(0, Integer.MAX_VALUE);
            String pattern = Integer.toString(idx, Character.MAX_RADIX);
            if (idx < patternCount
                || (pattern.startsWith("foo") && pattern.endsWith("bar"))  // 948437811
                || (pattern.startsWith("baz") && pattern.endsWith("bort")) // Can't match, but you get the idea
            ) {
                assertTrue("matches " + pattern, run.run(pattern));
            } else {
                assertFalse("matches " + pattern, run.run(pattern));
            }
            assertTrue("matches " + pattern, run.run("foo" + pattern + "bar"));
            assertTrue("matches " + pattern, run.run("baz" + pattern + "bort"));
        }
    }

    private void assertMatchesAll(Automaton automaton, String... strings) {
        CharacterRunAutomaton run = new CharacterRunAutomaton(automaton);
        for (String s : strings) {
            assertTrue(run.run(s));
        }
    }

    private void assertMatchesNone(Automaton automaton, String... strings) {
        CharacterRunAutomaton run = new CharacterRunAutomaton(automaton);
        for (String s : strings) {
            assertFalse(run.run(s));
        }
    }

    public void testSimpleMatcher() {
        assertFalse(Regex.simpleMatcher((String[]) null).test("abc"));
        assertFalse(Regex.simpleMatcher().test("abc"));
        assertTrue(Regex.simpleMatcher("abc").test("abc"));
        assertFalse(Regex.simpleMatcher("abc").test("abd"));

        assertTrue(Regex.simpleMatcher("abc", "xyz").test("abc"));
        assertTrue(Regex.simpleMatcher("abc", "xyz").test("xyz"));
        assertFalse(Regex.simpleMatcher("abc", "xyz").test("abd"));
        assertFalse(Regex.simpleMatcher("abc", "xyz").test("xyy"));

        assertTrue(Regex.simpleMatcher("abc", "*").test("abc"));
        assertTrue(Regex.simpleMatcher("abc", "*").test("abd"));

        assertTrue(Regex.simpleMatcher("a*c").test("abc"));
        assertFalse(Regex.simpleMatcher("a*c").test("abd"));

        assertTrue(Regex.simpleMatcher("a*c", "x*z").test("abc"));
        assertTrue(Regex.simpleMatcher("a*c", "x*z").test("xyz"));
        assertFalse(Regex.simpleMatcher("a*c", "x*z").test("abd"));
        assertFalse(Regex.simpleMatcher("a*c", "x*z").test("xyy"));
    }
}
