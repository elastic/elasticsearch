/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StringPatternTests extends ESTestCase {

    private LikePattern like(String pattern, char escape) {
        return new LikePattern(pattern, escape);
    }

    private RLikePattern rlike(String pattern) {
        return new RLikePattern(pattern);
    }

    private boolean matchesAll(String pattern, char escape) {
        return like(pattern, escape).matchesAll();
    }

    private boolean exactMatch(String pattern, char escape) {
        String escaped = pattern.replace(Character.toString(escape), StringUtils.EMPTY);
        return escaped.equals(like(pattern, escape).exactMatch());
    }

    private boolean matchesAll(String pattern) {
        return rlike(pattern).matchesAll();
    }

    private boolean exactMatch(String pattern) {
        return pattern.equals(rlike(pattern).exactMatch());
    }

    public void testWildcardMatchAll() throws Exception {
        assertTrue(matchesAll("%", '0'));
        assertTrue(matchesAll("%%", '0'));

        assertFalse(matchesAll("a%", '0'));
        assertFalse(matchesAll("%_", '0'));
        assertFalse(matchesAll("%_%_%", '0'));
        assertFalse(matchesAll("_%", '0'));
        assertFalse(matchesAll("0%", '0'));
    }

    public void testRegexMatchAll() throws Exception {
        assertTrue(matchesAll(".*"));
        assertTrue(matchesAll(".*.*"));
        assertTrue(matchesAll(".*.?"));
        assertTrue(matchesAll(".?.*"));
        assertTrue(matchesAll(".*.?.*"));

        assertFalse(matchesAll("..*"));
        assertFalse(matchesAll("ab."));
        assertFalse(matchesAll("..?"));
    }

    public void testWildcardExactMatch() throws Exception {
        assertTrue(exactMatch("0%", '0'));
        assertTrue(exactMatch("0_", '0'));
        assertTrue(exactMatch("123", '0'));
        assertTrue(exactMatch("1230_", '0'));
        assertTrue(exactMatch("1230_321", '0'));

        assertFalse(exactMatch("%", '0'));
        assertFalse(exactMatch("%%", '0'));
        assertFalse(exactMatch("a%", '0'));
        assertFalse(exactMatch("a_", '0'));
    }

    public void testRegexExactMatch() throws Exception {
        assertFalse(exactMatch(".*"));
        assertFalse(exactMatch(".*.*"));
        assertFalse(exactMatch(".*.?"));
        assertFalse(exactMatch(".?.*"));
        assertFalse(exactMatch(".*.?.*"));
        assertFalse(exactMatch("..*"));
        assertFalse(exactMatch("ab."));
        assertFalse(exactMatch("..?"));

        assertTrue(exactMatch("abc"));
        assertTrue(exactMatch("12345"));
    }

    /**
     * Lucene parses nested groups recursively, so a deep pattern overflows the stack while parsing; on a coordinator thread
     * that Error would take the node down. Within the length limit the nesting is bounded, so the overflow is only certain
     * on a thread with a small fixed stack, which also keeps the test independent of the platform's default stack size.
     */
    public void testDeeplyNestedRegexIsAClientError() {
        int depth = (AbstractStringPattern.MAX_PATTERN_LENGTH - 1) / 2;
        String regex = "(".repeat(depth) + "a" + ")".repeat(depth);
        assertOnSmallStack(() -> rlike(regex).createAutomaton(), "Pattern nesting is too deep to evaluate");
    }

    public void testTooComplexRegexIsAClientError() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> rlike("(a|b)*a(a|b){30}").createAutomaton());
        assertEquals("Pattern was too complex to determinize", e.getMessage());
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
    }

    /**
     * 22 characters and about a billion NFA states: the estimate must refuse it before any of it is allocated. Building it
     * would exhaust the test JVM, so the test also proves the refusal precedes the build.
     */
    public void testHugeRegexIsRefusedBeforeItIsBuilt() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> rlike("[ab]{1000}{1000}{1000}").createAutomaton());
        assertThat(e.getMessage(), containsString("Pattern is too large to compile"));
    }

    public void testRepeatCountOutOfRangeIsAClientError() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> rlike("a{99999999999}").createAutomaton());
        assertEquals("Pattern repeat count is out of range", e.getMessage());
    }

    public void testPatternLongerThanLimitIsRejected() {
        String tooLong = "a".repeat(AbstractStringPattern.MAX_PATTERN_LENGTH + 1);
        for (AbstractStringPattern pattern : new AbstractStringPattern[] {
            rlike(tooLong),
            like(tooLong, '0'),
            new WildcardPattern(tooLong) }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, pattern::createAutomaton);
            assertThat(e.getMessage(), containsString("Pattern length [" + tooLong.length() + "] exceeds the allowed maximum"));
        }
        String atLimit = "a".repeat(AbstractStringPattern.MAX_PATTERN_LENGTH);
        assertEquals(atLimit, rlike(atLimit).exactMatch());
        assertEquals(atLimit, like(atLimit, '0').exactMatch());
    }

    private static void assertOnSmallStack(Runnable compile, String expectedMessage) {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                compile.run();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "small-stack-regex", 256 * 1024);
        thread.setDaemon(true);
        thread.start();
        try {
            thread.join(TimeValue.timeValueSeconds(30).millis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        assertFalse("pattern compilation did not finish", thread.isAlive());
        assertThat(thrown.get(), instanceOf(IllegalArgumentException.class));
        assertEquals(expectedMessage, thrown.get().getMessage());
    }
}
