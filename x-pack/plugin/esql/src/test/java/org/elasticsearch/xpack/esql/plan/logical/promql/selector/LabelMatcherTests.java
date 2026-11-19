/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LabelMatcherTests extends ESTestCase {

    public void testSingleValueConstructor() {
        LabelMatcher matcher = new LabelMatcher("host", "server-1", Matcher.EQ);
        assertThat(matcher.name(), equalTo("host"));
        assertThat(matcher.getFirstValue(), equalTo("server-1"));
        assertThat(matcher.values(), equalTo(List.of("server-1")));
        assertThat(matcher.isMultiValue(), equalTo(false));
        assertThat(matcher.matcher(), equalTo(Matcher.EQ));
    }

    public void testMultiValueConstructor() {
        LabelMatcher matcher = new LabelMatcher("host", List.of("server-1", "server-2"), Matcher.EQ);
        assertThat(matcher.name(), equalTo("host"));
        assertThat(matcher.getFirstValue(), equalTo("server-1"));
        assertThat(matcher.values(), equalTo(List.of("server-1", "server-2")));
        assertThat(matcher.isMultiValue(), equalTo(true));
        assertThat(matcher.matcher(), equalTo(Matcher.EQ));
    }

    public void testSingleValueExactMatchesLiteral() {
        LabelMatcher matcher = new LabelMatcher("host", "server-1", Matcher.EQ);
        assertTrue(Operations.run(matcher.automaton(), "server-1"));
        assertFalse(Operations.run(matcher.automaton(), "server-2"));
        assertFalse(Operations.run(matcher.automaton(), "server-1-extra"));
    }

    public void testSingleValueExactMatchesDotsLiterally() {
        LabelMatcher matcher = new LabelMatcher("field", "k8s.pod.name", Matcher.EQ);
        assertTrue(Operations.run(matcher.automaton(), "k8s.pod.name"));
        assertFalse(Operations.run(matcher.automaton(), "k8sXpodXname"));
        assertFalse(Operations.run(matcher.automaton(), "k8s-pod-name"));
    }

    public void testSingleValueRegexMatches() {
        LabelMatcher matcher = new LabelMatcher("host", "server-.*", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "server-1"));
        assertTrue(Operations.run(matcher.automaton(), "server-abc"));
        assertFalse(Operations.run(matcher.automaton(), "web-1"));
    }

    public void testSingleValueNegationExact() {
        LabelMatcher matcher = new LabelMatcher("env", "prod", Matcher.NEQ);
        assertFalse(Operations.run(matcher.automaton(), "prod"));
        assertTrue(Operations.run(matcher.automaton(), "dev"));
        assertTrue(Operations.run(matcher.automaton(), "staging"));
    }

    public void testSingleValueNegationRegex() {
        LabelMatcher matcher = new LabelMatcher("env", "test.*", Matcher.NREG);
        assertFalse(Operations.run(matcher.automaton(), "test"));
        assertFalse(Operations.run(matcher.automaton(), "test-1"));
        assertTrue(Operations.run(matcher.automaton(), "prod"));
    }

    public void testMultiValueExactMatches() {
        LabelMatcher matcher = new LabelMatcher("host", List.of("cat", "dog"), Matcher.EQ);
        assertTrue(Operations.run(matcher.automaton(), "cat"));
        assertTrue(Operations.run(matcher.automaton(), "dog"));
        assertFalse(Operations.run(matcher.automaton(), "bird"));
        assertFalse(Operations.run(matcher.automaton(), "cat|dog"));
    }

    public void testMultiValueExactWithSpecialChars() {
        LabelMatcher matcher = new LabelMatcher("field", List.of("k8s.pod.name", "service.api"), Matcher.EQ);
        assertTrue(Operations.run(matcher.automaton(), "k8s.pod.name"));
        assertTrue(Operations.run(matcher.automaton(), "service.api"));
        assertFalse(Operations.run(matcher.automaton(), "k8sXpodXname"));
        assertFalse(Operations.run(matcher.automaton(), "serviceXapi"));
    }

    public void testMultiValueRegexMatches() {
        LabelMatcher matcher = new LabelMatcher("host", List.of("server-.*", "web-[0-9]+"), Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "server-1"));
        assertTrue(Operations.run(matcher.automaton(), "server-abc"));
        assertTrue(Operations.run(matcher.automaton(), "web-123"));
        assertFalse(Operations.run(matcher.automaton(), "web-abc"));
        assertFalse(Operations.run(matcher.automaton(), "db-1"));
    }

    public void testMultiValueNegationExact() {
        LabelMatcher matcher = new LabelMatcher("env", List.of("test", "dev"), Matcher.NEQ);
        assertFalse(Operations.run(matcher.automaton(), "test"));
        assertFalse(Operations.run(matcher.automaton(), "dev"));
        assertTrue(Operations.run(matcher.automaton(), "prod"));
        assertTrue(Operations.run(matcher.automaton(), "staging"));
    }

    public void testMultiValueNegationRegex() {
        LabelMatcher matcher = new LabelMatcher("env", List.of("test.*", "dev.*"), Matcher.NREG);
        assertFalse(Operations.run(matcher.automaton(), "test"));
        assertFalse(Operations.run(matcher.automaton(), "test-1"));
        assertFalse(Operations.run(matcher.automaton(), "dev"));
        assertFalse(Operations.run(matcher.automaton(), "dev-local"));
        assertTrue(Operations.run(matcher.automaton(), "prod"));
        assertTrue(Operations.run(matcher.automaton(), "staging"));
    }

    public void testMatchesEmpty() {
        LabelMatcher empty = new LabelMatcher("label", "", Matcher.EQ);
        assertTrue(empty.matchesEmpty());

        LabelMatcher nonEmpty = new LabelMatcher("label", "value", Matcher.EQ);
        assertFalse(nonEmpty.matchesEmpty());

        LabelMatcher notFoo = new LabelMatcher("label", "foo", Matcher.NEQ);
        assertTrue(notFoo.matchesEmpty());
    }

    public void testMatchesAll() {
        LabelMatcher all = new LabelMatcher("label", ".*", Matcher.REG);
        assertTrue(all.matchesAll());

        LabelMatcher specific = new LabelMatcher("label", "value", Matcher.EQ);
        assertFalse(specific.matchesAll());
    }

    public void testMatchesNone() {
        LabelMatcher none = new LabelMatcher("label", ".*", Matcher.NREG);
        assertTrue(none.matchesNone());

        LabelMatcher specific = new LabelMatcher("label", "value", Matcher.EQ);
        assertFalse(specific.matchesNone());
    }

    public void testIsNegation() {
        assertFalse(new LabelMatcher("l", "v", Matcher.EQ).isNegation());
        assertTrue(new LabelMatcher("l", "v", Matcher.NEQ).isNegation());
        assertFalse(new LabelMatcher("l", "v", Matcher.REG).isNegation());
        assertTrue(new LabelMatcher("l", "v", Matcher.NREG).isNegation());
    }

    public void testEqualsAndHashCode() {
        LabelMatcher m1 = new LabelMatcher("host", List.of("a", "b"), Matcher.EQ);
        LabelMatcher m2 = new LabelMatcher("host", List.of("a", "b"), Matcher.EQ);
        LabelMatcher m3 = new LabelMatcher("host", List.of("a", "c"), Matcher.EQ);
        LabelMatcher m4 = new LabelMatcher("host", List.of("a", "b"), Matcher.NEQ);

        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
        assertNotEquals(m1, m3);
        assertNotEquals(m1, m4);
    }

    public void testToString() {
        LabelMatcher single = new LabelMatcher("host", "server-1", Matcher.EQ);
        assertThat(single.toString(), equalTo("host=[server-1]"));

        LabelMatcher multi = new LabelMatcher("env", List.of("test", "dev"), Matcher.NEQ);
        assertThat(multi.toString(), equalTo("env!=[test, dev]"));

        LabelMatcher regex = new LabelMatcher("host", "server-.*", Matcher.REG);
        assertThat(regex.toString(), equalTo("host=~[server-.*]"));
    }

    public void testMatcherFrom() {
        assertThat(Matcher.from("="), equalTo(Matcher.EQ));
        assertThat(Matcher.from("!="), equalTo(Matcher.NEQ));
        assertThat(Matcher.from("=~"), equalTo(Matcher.REG));
        assertThat(Matcher.from("!~"), equalTo(Matcher.NREG));
        assertNull(Matcher.from("invalid"));
    }

    public void testMatcherIsRegex() {
        assertFalse(Matcher.EQ.isRegex());
        assertFalse(Matcher.NEQ.isRegex());
        assertTrue(Matcher.REG.isRegex());
        assertTrue(Matcher.NREG.isRegex());
    }
}
