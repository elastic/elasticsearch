/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
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
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton, DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertTrue(runAutomaton.run(text));
    }

    private void assertMismatch(Automaton automaton, String text) {
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton, DEFAULT_DETERMINIZE_WORK_LIMIT);
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

    public void testSettingMaxDeterminizedStates() {
        try {
            assertNotEquals(10000, Automatons.getMaxDeterminizedStates());
            // set to the min value
            Settings settings = Settings.builder().put(Automatons.MAX_DETERMINIZED_STATES_SETTING.getKey(), 10000).build();
            Automatons.updateConfiguration(settings);
            assertEquals(10000, Automatons.getMaxDeterminizedStates());

            final List<String> names = new ArrayList<>(4096);
            for (int i = 0; i < 4096; i++) {
                names.add(randomAlphaOfLength(64));
            }
            TooComplexToDeterminizeException e = expectThrows(TooComplexToDeterminizeException.class, () -> Automatons.patterns(names));
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

    // This isn't use directly in the code, but it's sometimes needed when debugging failing tests
    // (and it is annoying to have to rewrite it each time it's needed)
    public static <A extends Appendable> A debug(Automaton a, A out) throws IOException {
        out.append("Automaton {");
        out.append(String.format(Locale.ROOT, "States:%d  Deterministic:%s", a.getNumStates(), a.isDeterministic()));
        for (int s = 0; s < a.getNumStates(); s++) {
            out.append(String.format(Locale.ROOT, " [State#%d %s", s, a.isAccept(s) ? "(accept)" : ""));
            for (int t = 0; t < a.getNumTransitions(s); t++) {
                Transition transition = new Transition();
                a.getTransition(s, t, transition);
                out.append(String.format(Locale.ROOT, " (%05d - %05d => %s)", transition.min, transition.max, transition.dest));
            }
            out.append("]");
        }
        return out;
    }
}
