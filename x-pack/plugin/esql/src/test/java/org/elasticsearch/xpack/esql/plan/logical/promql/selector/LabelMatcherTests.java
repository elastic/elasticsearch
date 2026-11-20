/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

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

    /**
     * Nested bounded repeats expand far beyond the pattern's length. RE2 refuses them while parsing, as Prometheus does:
     * a repeat directly on a repeat is invalid, and repeats nested through groups may expand to at most 1000 copies. So
     * the pattern is a client error before any automaton is built, in a single-value and in a multi-value matcher alike.
     */
    public void testHugeRegexIsRefusedBeforeItIsBuilt() {
        String regex = randomFrom("[ab]{1000}{900}{1000}", "(?:(?:[ab]{1000}){900}){1000}");
        for (LabelMatcher matcher : List.of(
            new LabelMatcher("l", regex, Matcher.REG),
            new LabelMatcher("l", List.of("a", regex), Matcher.NREG)
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, matcher::automaton);
            assertThat(e.getMessage(), equalTo("Cannot parse regex " + regex));
            assertThat(e.getCause().getMessage(), containsString("repe"));
        }
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

    /** RE2 reads Lucene-only operators as literals: empty language, intersection, interval, any string, complement. */
    public void testRegexLuceneOnlyOperatorsAreLiterals() {
        for (String regex : List.of("#", "a&b", "a<1-3>", "@", "~a")) {
            LabelMatcher matcher = new LabelMatcher("label", regex, Matcher.REG);
            assertThat(regex, AutomatonUtils.matchesExact(matcher.automaton()), equalTo(regex));

            LabelMatcher negated = new LabelMatcher("label", regex, Matcher.NREG);
            assertFalse(regex, Operations.run(negated.automaton(), regex));
            assertTrue(regex, Operations.run(negated.automaton(), "q"));
            assertFalse(regex, negated.matchesAll());
        }
    }

    public void testRegexLuceneOnlyOperatorsInsideExpression() {
        LabelMatcher matcher = new LabelMatcher("label", "(a<1-3>|b)", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "a<1-3>"));
        assertTrue(Operations.run(matcher.automaton(), "b"));
        assertFalse(Operations.run(matcher.automaton(), "a1"));

        matcher = new LabelMatcher("label", "x|#|a.*@", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "x"));
        assertTrue(Operations.run(matcher.automaton(), "#"));
        assertTrue(Operations.run(matcher.automaton(), "abc@"));
        assertFalse(Operations.run(matcher.automaton(), "abc"));
        assertFalse(matcher.matchesEmpty());
    }

    public void testRegexEscapes() {
        // an escaped operator stays a single literal
        LabelMatcher matcher = new LabelMatcher("label", "a\\<1-3>\\&", Matcher.REG);
        assertThat(AutomatonUtils.matchesExact(matcher.automaton()), equalTo("a<1-3>&"));
        // an escaped backslash does not escape the operator after it
        matcher = new LabelMatcher("label", "\\\\#", Matcher.REG);
        assertThat(AutomatonUtils.matchesExact(matcher.automaton()), equalTo("\\#"));
    }

    public void testRegexCharacterClass() {
        LabelMatcher matcher = new LabelMatcher("label", "[#-@]", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "#"));
        assertTrue(Operations.run(matcher.automaton(), "1"));
        assertTrue(Operations.run(matcher.automaton(), "@"));
        assertFalse(Operations.run(matcher.automaton(), "a"));

        // a ']' right after '[' or '[^' is a literal; the '&' after the class is an RE2 literal again
        matcher = new LabelMatcher("label", "[]~]&", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "]&"));
        assertTrue(Operations.run(matcher.automaton(), "~&"));
        matcher = new LabelMatcher("label", "[^]]&", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "a&"));
        assertFalse(Operations.run(matcher.automaton(), "]&"));
    }

    /** RE2 syntax that Lucene's regexp syntax lacks or reads otherwise. */
    public void testRegexRe2Syntax() {
        LabelMatcher matcher = new LabelMatcher("label", "(?i)(?:prod|stag)-\\d+", Matcher.REG);
        assertTrue(Operations.run(matcher.automaton(), "PROD-12"));
        assertFalse(Operations.run(matcher.automaton(), "prod-"));

        matcher = new LabelMatcher("label", "\"a\"\\n", Matcher.REG);
        assertThat(AutomatonUtils.matchesExact(matcher.automaton()), equalTo("\"a\"\n"));
    }

    /** The filter gets the regex as written, in RE2 syntax like the automaton. */
    public void testRegexPattern() {
        Re2Pattern pattern = new Re2Pattern("(?i)(a<1-3>|~b&c)");
        assertThat(pattern.pattern(), equalTo("(?i)(a<1-3>|~b&c)"));
        Automaton expected = new LabelMatcher("label", "(?i)(a<1-3>|~b&c)", Matcher.REG).automaton();
        assertTrue(AutomatonTestUtil.sameLanguage(expected, pattern.createAutomaton(false)));
    }

    public void testInvalidRegex() {
        for (String regex : List.of("(", "[a", "\\", "a**", "(?=a)")) {
            LabelMatcher matcher = new LabelMatcher("label", regex, Matcher.REG);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, matcher::automaton);
            assertThat(e.getMessage(), equalTo("Cannot parse regex " + regex));
            assertNotNull("the RE2 parse error is kept as the cause", e.getCause());
        }
    }

    /** The value of {@code !=} is a literal, so regex metacharacters in it, as in {@code le!="+Inf"}, match themselves. */
    public void testNegatedLiteralIsNotARegex() {
        String value = randomFrom("+Inf", "*", "a(b", "[x", "a|b");
        Expression condition = LabelMatchers.condition(
            Source.EMPTY,
            new ReferenceAttribute(Source.EMPTY, "le", DataType.KEYWORD),
            new LabelMatcher("le", value, Matcher.NEQ)
        );
        assertFalse(condition.anyMatch(e -> e instanceof Re2Like));
        assertTrue(condition.anyMatch(e -> e instanceof Literal literal && value.equals(BytesRefs.toString(literal.value()))));
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

    /**
     * The length bound the regexp query applies through {@code index.max_regex_length}, at its default, since a label
     * matcher has no index to read it from. Checked per pattern, so one long value in a multi-value matcher is enough.
     */
    public void testOverLongRegexIsAClientError() {
        String tooLong = "a".repeat(LabelMatcher.MAX_REGEX_LENGTH + 1);
        for (LabelMatcher matcher : List.of(
            new LabelMatcher("host", tooLong, Matcher.REG),
            new LabelMatcher("host", List.of("server-.*", tooLong), Matcher.NREG)
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, matcher::automaton);
            assertThat(e.getMessage(), containsString("The length of regex [" + tooLong.length() + "]"));
            assertThat(e.getMessage(), containsString("allowed maximum of [" + LabelMatcher.MAX_REGEX_LENGTH + "]"));
        }
    }

    /**
     * Parsing succeeds on this pattern; the blow-up happens when the NFA is determinized, which must be a
     * client error too, not a 500. The negated matcher takes the same path, since its complement needs the DFA first.
     */
    public void testTooComplexRegexIsAClientError() {
        for (Matcher m : List.of(Matcher.REG, Matcher.NREG)) {
            LabelMatcher matcher = new LabelMatcher("host", "(a|b)*a(a|b){30}", m);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, matcher::automaton);
            assertThat(e.getMessage(), containsString("too complex to determinize"));
            assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
        }
    }

    public void testMultiValueInvalidRegexIsReported() {
        LabelMatcher matcher = new LabelMatcher("host", List.of("server-.*", "web-("), Matcher.REG);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, matcher::automaton);
        assertThat(e.getMessage(), containsString("Cannot parse regex web-("));
        assertNotNull("the RE2 parse error is kept as the cause", e.getCause());
    }

    /**
     * Compiling nested groups recurses, so a deep pattern overflows the stack. It must surface as a client error,
     * not an {@link Error}. Runs on a thread with a fixed, small stack so the depth needed does not depend on the platform's
     * default stack size.
     */
    public void testDeeplyNestedRegexIsAClientError() throws Exception {
        int depth = 20_000;
        String regex = "(".repeat(depth) + "a" + ")".repeat(depth);
        // The length bound rejects this pattern first; lifting it is what makes the overflow guard itself observable.
        for (LabelMatcher matcher : List.of(
            new LabelMatcher("host", List.of(regex), Matcher.REG, Integer.MAX_VALUE),
            new LabelMatcher("host", List.of("server-.*", regex), Matcher.NREG, Integer.MAX_VALUE)
        )) {
            AtomicReference<Throwable> thrown = new AtomicReference<>();
            Thread thread = new Thread(null, () -> {
                try {
                    matcher.automaton();
                } catch (Throwable t) {
                    thrown.set(t);
                }
            }, "small-stack-regex", 256 * 1024);
            thread.setDaemon(true);
            thread.start();
            thread.join(TimeValue.timeValueSeconds(30).millis());
            assertFalse("regex compilation did not finish", thread.isAlive());
            assertThat(thrown.get(), instanceOf(IllegalArgumentException.class));
            assertThat(thrown.get().getMessage(), containsString("too deeply nested"));
        }
    }
}
