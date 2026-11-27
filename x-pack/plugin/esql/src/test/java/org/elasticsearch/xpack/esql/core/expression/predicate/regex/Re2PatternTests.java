/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * The automaton a shard builds for {@code Re2Like} is the one {@link Re2Pattern#createAutomaton(boolean)} builds, with
 * the memory of determinizing it accounted on the shard's circuit breaker.
 */
public class Re2PatternTests extends ESTestCase {

    public void testBreakerAutomatonIsTheSame() {
        Re2Pattern pattern = new Re2Pattern(randomFrom("(a|b)*a(a|b){3}", ".*-prod", "(?i)error|warn(ing)?", "\\pL+_total", "foo|bar"));
        boolean ignoreCase = randomBoolean();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        Automaton accounted = pattern.createAutomaton(ignoreCase, breaker, "test");
        assertTrue(AutomatonTestUtil.sameLanguage(pattern.createAutomaton(ignoreCase), accounted));
        assertThat("the memory of determinizing is released", breaker.getUsed(), equalTo(0L));
    }

    /** An expression whose deterministic automaton has thousands of states trips a small breaker. */
    public void testTripsBreaker() {
        Re2Pattern pattern = new Re2Pattern("(a|b)*a(a|b){12}");
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofKb(64));
        expectThrows(CircuitBreakingException.class, () -> pattern.createAutomaton(false, breaker, "test"));
        assertThat("the memory of determinizing is released", breaker.getUsed(), equalTo(0L));
    }
}
