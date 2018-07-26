/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.support;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;
import static org.elasticsearch.xpack.core.security.support.Automatons.pattern;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;
import static org.elasticsearch.xpack.core.security.support.Automatons.predicate;
import static org.elasticsearch.xpack.core.security.support.Automatons.wildcard;
import static org.hamcrest.Matchers.equalTo;

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

    public void testPredicateToString() throws Exception {
        assertThat(predicate("a.*z").toString(), equalTo("a.*z"));
        assertThat(predicate("a.*z", "A.*Z").toString(), equalTo("a.*z|A.*Z"));
        assertThat(predicate("a.*z", "A.*Z", "Α.*Ω").toString(), equalTo("a.*z|A.*Z|Α.*Ω"));
    }

    public void testPatternComplexity() {
        List<String> patterns = Arrays.asList("*", "filebeat*de-tst-chatclassification*",
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
                "heartbeat*bender-minio-test-1*");
        final Automaton automaton = Automatons.patterns(patterns);
        assertTrue(Operations.isTotal(automaton));
        assertTrue(automaton.isDeterministic());
    }

    private void assertMatch(Automaton automaton, String text) {
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton, DEFAULT_MAX_DETERMINIZED_STATES);
        assertTrue(runAutomaton.run(text));
    }

    private void assertMismatch(Automaton automaton, String text) {
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton, DEFAULT_MAX_DETERMINIZED_STATES);
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
            Automatons.updateMaxDeterminizedStates(settings);
            assertEquals(10000, Automatons.getMaxDeterminizedStates());

            final List<String> names = new ArrayList<>(1024);
            for (int i = 0; i < 1024; i++) {
                names.add(randomAlphaOfLength(48));
            }
            TooComplexToDeterminizeException e = expectThrows(TooComplexToDeterminizeException.class, () -> Automatons.patterns(names));
            assertThat(e.getMaxDeterminizedStates(), equalTo(10000));
        } finally {
            Automatons.updateMaxDeterminizedStates(Settings.EMPTY);
            assertEquals(100000, Automatons.getMaxDeterminizedStates());
        }
    }
}
