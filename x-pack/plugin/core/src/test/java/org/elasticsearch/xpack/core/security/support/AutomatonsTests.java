/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.support.Automatons.pattern;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;
import static org.elasticsearch.xpack.core.security.support.Automatons.predicate;
import static org.elasticsearch.xpack.core.security.support.Automatons.wildcard;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AutomatonsTests extends ESTestCase {
    public void testPatternsUnionOfMultiplePatterns() throws Exception {
        assertMatch(patterns("/fo.*/", "ba*"), "foo");
        assertMatch(patterns("/fo.*/", "ba*"), "bar");
        assertMismatch(patterns("/fo.*/", "ba*"), "zipfoo");
    }

    public void testPatternSingle() throws Exception {
        assertMatch(pattern("/.*st/"), "test");
        assertMatch(pattern("/t.*st/"), "test");
        assertMatch(pattern("/tes*./"), "test");
        assertMatch(pattern("/test/"), "test");
        assertMismatch(pattern("/.*st/"), "tet");
        assertMatch(pattern("*st"), "test");
        assertMatch(pattern("t*t"), "test");
        assertMatch(pattern("t?st"), "test");
        assertMismatch(pattern("t?t"), "test");
        assertMatch(pattern("tes*"), "test");
        assertMatch(pattern("test"), "test");
        assertMismatch(pattern("*st"), "tet");
        assertInvalidPattern("/test");
        assertInvalidPattern("/te*");
        assertInvalidPattern("/te.*");
        assertMismatch(pattern(".*st"), "test");
        assertMatch(pattern("*st\\"), "test\\");
        assertMatch(pattern("tes.*/"), "tes.t/");
        assertMatch(pattern("\\/test"), "/test");
    }

    public void testWildcard() throws Exception {
        assertMatch(wildcard("*st"), "test");
        assertMatch(wildcard("t*st"), "test");
        assertMatch(wildcard("tes*"), "test");
        assertMatch(wildcard("test"), "test");
        assertMismatch(wildcard("*st"), "tet");
        assertMismatch(wildcard("t\\*st"), "test");
        assertMatch(wildcard("t\\*st"), "t*st");
    }

    public void testUnicode() throws Exception {
        assertMatch(wildcard("*ξη"), "λέξη");
        assertMatch(wildcard("сл*во"), "слово");
        assertMatch(wildcard("စကာ*"), "စကားလုံး");
        assertMatch(wildcard("וואָרט"), "וואָרט");
    }

    public void testSupplementaryCodePoints() {
        // Supplementary (non-BMP) code points occupy two Java chars (a UTF-16 surrogate pair). The builder must emit a single
        // code-point transition per code point so that CharacterRunAutomaton, which advances by code point, can match it.
        // Code-point handling here was inadvertently dropped in e7f141bd ("use brics automaton instead of lucene"); brics is a
        // char-based library, and the char-by-char iteration survived the later migration back to Lucene until it was restored.
        final String x = "\uD835\uDD4F"; // U+1D54F MATHEMATICAL DOUBLE-STRUCK CAPITAL X
        final String y = "\uD835\uDD50"; // U+1D550 MATHEMATICAL DOUBLE-STRUCK CAPITAL Y (a different supplementary code point)

        assertMatch(wildcard("foo" + x), "foo" + x);
        assertMismatch(wildcard("foo" + x), "foo" + y);
        assertMatch(wildcard("*" + x), "bar" + x);
        assertMatch(wildcard(x + "*"), x + "bar");
        // '?' matches a single code point, including a supplementary one
        assertMatch(wildcard("foo?"), "foo" + x);
        // an escaped supplementary literal is matched in full
        assertMatch(wildcard("\\" + x), x);
        // the multi-pattern union path matches a supplementary literal too
        assertMatch(patterns("a" + x, "b" + x), "a" + x);
    }

    public void testPredicateToString() throws Exception {
        assertThat(predicate("a.*z").toString(), equalTo("a.*z"));
        assertThat(predicate("a.*z", "A.*Z").toString(), equalTo("a.*z|A.*Z"));
        assertThat(predicate("a.*z", "A.*Z", "Α.*Ω").toString(), equalTo("a.*z|A.*Z|Α.*Ω"));
    }

    public void testPatternComplexity() {
        List<String> patterns = Arrays.asList(
            "*",
            "filebeat*de-tst-chatclassification*",
            "metricbeat*de-tst-chatclassification*",
            "packetbeat*de-tst-chatclassification*",
            "heartbeat*de-tst-chatclassification*",
            "filebeat*documentationdev*",
            "metricbeat*documentationdev*",
            "packetbeat*documentationdev*",
            "heartbeat*documentationdev*",
            "filebeat*devsupport-website*",
            "metricbeat*devsupport-website*",
            "packetbeat*devsupport-website*",
            "heartbeat*devsupport-website*",
            ".kibana-tcloud",
            ".reporting-tcloud",
            "filebeat-app-ingress-*",
            "filebeat-app-tcloud-*",
            "filebeat*documentationprod*",
            "metricbeat*documentationprod*",
            "packetbeat*documentationprod*",
            "heartbeat*documentationprod*",
            "filebeat*bender-minio-test-1*",
            "metricbeat*bender-minio-test-1*",
            "packetbeat*bender-minio-test-1*",
            "heartbeat*bender-minio-test-1*"
        );
        final Automaton automaton = Automatons.patterns(patterns);
        assertTrue(Operations.isTotal(automaton));
        assertTrue(automaton.isDeterministic());
    }

    private void assertMatch(Automaton automaton, String text) {
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue(runAutomaton.run(text));
    }

    private void assertMismatch(Automaton automaton, String text) {
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertFalse(runAutomaton.run(text));
    }

    private void assertInvalidPattern(String text) {
        try {
            pattern(text);
            fail("expected an error on invalid pattern [" + text + "]");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    public void testLotsOfIndices() {
        final int numberOfIndices = scaledRandomIntBetween(512, 1024);
        final List<String> names = new ArrayList<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            names.add(randomAlphaOfLengthBetween(6, 48));
        }
        final Automaton automaton = Automatons.patterns(names);
        assertTrue(automaton.isDeterministic());

        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        for (String name : names) {
            assertTrue(runAutomaton.run(name));
        }
    }

    public void testLiteralPartitionMatchesGeneralBuilderLanguage() {
        assertSameLanguageBothBuilders(List.of("read", "write", "manage"));
        assertSameLanguageBothBuilders(List.of("a", "a", "b"));
        assertSameLanguageBothBuilders(List.of("read"));
        assertSameLanguageBothBuilders(List.of("read", "write", "indices:data/read/*", "/clu.*ter/"));
        assertSameLanguageBothBuilders(List.of("indices:data/read/*", "/clu.*ter/"));
        assertSameLanguageBothBuilders(List.of("a", "*", "*foo*", "bar*", "*baz"));
        assertSameLanguageBothBuilders(List.of("foo\\*", "lit1", "lit2"));
        assertSameLanguageBothBuilders(List.of("fo?o*", "lit1", "lit2"));
        assertSameLanguageBothBuilders(List.of("", "lit1", "lit2"));
    }

    public void testLiteralPartitionMatchesGeneralBuilderLanguageRandomized() {
        for (int iter = 0; iter < 50; iter++) {
            final Set<String> input = new LinkedHashSet<>();
            final int count = randomIntBetween(1, 60);
            for (int i = 0; i < count; i++) {
                input.add(randomPattern());
            }
            assertSameLanguageBothBuilders(input);
        }
    }

    public void testLiteralPartitionMatchesGeneralBuilderForLongLiteral() {
        assertSameLanguageBothBuilders(List.of(randomAlphaOfLength(1000), "lit1", "lit2")); // at the bound: literal bucket
        assertSameLanguageBothBuilders(List.of(randomAlphaOfLength(1001), "lit1", "lit2")); // over the bound: general path
    }

    public void testLiteralPartitionMatchesGeneralBuilderForLongNonAsciiLiteral() {
        final String twoByteChar = "\u00e9"; // 'é' encodes to 2 bytes in UTF-8
        final String atByteBound = twoByteChar.repeat(500);   // 1000 UTF-8 bytes: still eligible for the literal bucket
        final String overByteBound = twoByteChar.repeat(501); // 1002 UTF-8 bytes: must fall to the general path
        assertEquals(1000, new BytesRef(atByteBound).length);
        assertEquals(1002, new BytesRef(overByteBound).length);
        assertSameLanguageBothBuilders(List.of(atByteBound, "lit1", "lit2"));
        assertSameLanguageBothBuilders(List.of(overByteBound, "lit1", "lit2"));
    }

    private static void assertSameLanguageBothBuilders(Collection<String> patterns) {
        final Automaton original = Automatons.buildPatternsAutomaton(patterns, false);
        final Automaton partitioned = Automatons.buildPatternsAutomaton(patterns, true);
        assertTrue(
            "literal partition diverged from the general builder for " + patterns,
            AutomatonTestUtil.sameLanguage(original, partitioned)
        );
        // CharacterRunAutomaton matching requires a deterministic automaton.
        assertTrue("literal partition produced a non-deterministic automaton for " + patterns, partitioned.isDeterministic());
    }

    private String randomPattern() {
        final String body = randomAlphaOfLengthBetween(1, 8);
        return switch (between(0, 7)) {
            case 0 -> body;                                      // literal
            case 1 -> body + "*";                                // trailing wildcard
            case 2 -> "*" + body;                                // leading wildcard
            case 3 -> "*" + body + "*";                          // infix wildcard
            case 4 -> body.charAt(0) + "?" + body.substring(1);  // embedded single-char wildcard
            case 5 -> "/" + body + ".*/";                        // lucene regex
            case 6 -> body + "\\*";                              // escaped trailing star (literal "*")
            case 7 -> "";                                        // empty string (excluded from the fast path)
            default -> throw new AssertionError("unreachable");
        };
    }

    public void testSettingMaxDeterminizedStates() {
        try {
            assertNotEquals(10000, Automatons.getMaxDeterminizedStates());
            // set to the min value
            Settings settings = Settings.builder().put(Automatons.MAX_DETERMINIZED_STATES_SETTING.getKey(), 10000).build();
            Automatons.updateConfiguration(settings);
            assertEquals(10000, Automatons.getMaxDeterminizedStates());

            final List<String> literals = new ArrayList<>(4096);
            for (int i = 0; i < 4096; i++) {
                literals.add(randomAlphaOfLength(64));
            }
            // A large all-literal union is compiled via Automata.makeStringUnion, which builds a minimal DFA directly without
            // determinization, so the max_determinized_states work limit does not apply and the build succeeds.
            final CharacterRunAutomaton literalRun = new CharacterRunAutomaton(Automatons.patterns(literals));
            for (String literal : literals) {
                assertTrue(literalRun.run(literal));
            }

            // Non-literal patterns still go through the general builder's determinization, which honors the work limit and
            // fails fast when exceeded. Prefixing each literal with "*" turns the same large set into suffix patterns.
            final List<String> wildcards = new ArrayList<>(4096);
            for (String literal : literals) {
                wildcards.add("*" + literal);
            }
            TooComplexToDeterminizeException e = expectThrows(TooComplexToDeterminizeException.class, () -> Automatons.patterns(wildcards));
            assertThat(e.getDeterminizeWorkLimit(), equalTo(10000));
        } finally {
            Automatons.updateConfiguration(Settings.EMPTY);
            assertEquals(100000, Automatons.getMaxDeterminizedStates());
        }
    }

    public void testCachingOfAutomatons() {
        Automatons.updateConfiguration(Settings.EMPTY);

        String pattern1 = randomAlphaOfLengthBetween(3, 8) + "*";
        String pattern2 = "/" + randomAlphaOfLengthBetween(1, 2) + "*" + randomAlphaOfLengthBetween(2, 4) + "/";

        final Automaton a1 = Automatons.pattern(pattern1);
        final Automaton a2 = Automatons.pattern(pattern2);

        assertThat(Automatons.pattern(pattern1), sameInstance(a1));
        assertThat(Automatons.pattern(pattern2), sameInstance(a2));

        final Automaton a3 = Automatons.patterns(pattern1, pattern2);
        final Automaton a4 = Automatons.patterns(pattern2, pattern1);
        assertThat(a3, sameInstance(a4));
    }

    public void testConfigurationOfCacheSize() {
        final Settings settings = Settings.builder().put(Automatons.CACHE_SIZE.getKey(), 2).build();
        Automatons.updateConfiguration(settings);

        String pattern1 = "a";
        String pattern2 = "b";
        String pattern3 = "c";

        final Automaton a1 = Automatons.pattern(pattern1);
        final Automaton a2 = Automatons.pattern(pattern2);

        assertThat(Automatons.pattern(pattern1), sameInstance(a1));
        assertThat(Automatons.pattern(pattern2), sameInstance(a2));

        final Automaton a3 = Automatons.pattern(pattern3);
        assertThat(Automatons.pattern(pattern3), sameInstance(a3));

        // either pattern 1 or 2 should be evicted (in theory it should be 1, but we don't care about that level of precision)
        final Automaton a1b = Automatons.pattern(pattern1);
        final Automaton a2b = Automatons.pattern(pattern2);
        if (a1b == a1 && a2b == a2) {
            fail("Expected one of the existing automatons to be evicted, but both were still cached");
        }
    }

    public void testDisableCache() {
        final Settings settings = Settings.builder().put(Automatons.CACHE_ENABLED.getKey(), false).build();
        Automatons.updateConfiguration(settings);

        final String pattern = randomAlphaOfLengthBetween(5, 10);
        final Automaton automaton = Automatons.pattern(pattern);
        assertThat(Automatons.pattern(pattern), not(sameInstance(automaton)));
    }

    public void testIsLiteralPattern() {
        // Plain strings are literal
        assertTrue(Automatons.isLiteralPattern("foo"));
        assertTrue(Automatons.isLiteralPattern("ec:/organizations:123/deployment:abc"));
        assertTrue(Automatons.isLiteralPattern("space:space42"));
        assertTrue(Automatons.isLiteralPattern("a/b/c"));
        assertTrue(Automatons.isLiteralPattern(""));

        // Wildcards are not literal
        assertFalse(Automatons.isLiteralPattern("*"));
        assertFalse(Automatons.isLiteralPattern("foo*"));
        assertFalse(Automatons.isLiteralPattern("*foo"));
        assertFalse(Automatons.isLiteralPattern("fo*o"));
        assertFalse(Automatons.isLiteralPattern("foo?"));
        assertFalse(Automatons.isLiteralPattern("f?o"));

        // Escape sequences are not literal
        assertFalse(Automatons.isLiteralPattern("foo\\*"));
        assertFalse(Automatons.isLiteralPattern("\\foo"));

        // Leading slash (Lucene regex) is not literal
        assertFalse(Automatons.isLiteralPattern("/foo/"));
        assertFalse(Automatons.isLiteralPattern("/foo.*/"));
        assertFalse(Automatons.isLiteralPattern("/"));
    }

    // This isn't use directly in the code, but it's sometimes needed when debugging failing tests
    // (and it is annoying to have to rewrite it each time it's needed)
    public static <A extends Appendable> A debug(Automaton a, A out) throws IOException {
        out.append("Automaton {");
        out.append(Strings.format("States:%d  Deterministic:%s", a.getNumStates(), a.isDeterministic()));
        for (int s = 0; s < a.getNumStates(); s++) {
            Object[] args = new Object[] { s, a.isAccept(s) ? "(accept)" : "" };
            out.append(Strings.format(" [State#%d %s", args));
            for (int t = 0; t < a.getNumTransitions(s); t++) {
                Transition transition = new Transition();
                a.getTransition(s, t, transition);
                out.append(Strings.format(" (%05d - %05d => %s)", transition.min, transition.max, transition.dest));
            }
            out.append("]");
        }
        return out;
    }
}
