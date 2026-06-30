/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Grok;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that expensive GROK patterns are interrupted when a real {@link MatcherWatchdog} is used.
 */
public class GrokWatchdogTimeoutTests extends ESTestCase {

    public void testWatchdogInterruptsBacktracking() {
        // With timeout 0 the watchdog fires immediately after the first check.
        Grok.Parser parser = Grok.pattern(Source.EMPTY, "(?<a>a+)+b", MatcherWatchdog.newInstance(0));
        RuntimeException ex = expectThrows(RuntimeException.class, () -> parser.grok().match("aaaaaaaaX"));
        assertThat(ex.getMessage(), containsString("interrupted"));
    }

    public void testNoopWatchdogDoesNotInterruptSimplePattern() {
        Grok.Parser parser = Grok.pattern(Source.EMPTY, "%{IP:client_ip}", MatcherWatchdog.noop());
        assertNotNull(parser.grok().captures("192.168.1.1"));
    }
}
