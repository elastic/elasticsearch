/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;

public class GrokTests extends ESTestCase {

    public void testWatchdogInterruptsBacktracking() {
        // This branch predates elastic/elasticsearch#139405, so MatcherWatchdog only has the older
        // thread-polling implementation (it interrupts a stuck matcher the next time the poll thread
        // wakes up, rather than via an exact joni-level timeout). Use a pattern whose catastrophic
        // backtracking (2^30 steps) vastly outlasts the short poll interval below, so the interrupt is
        // guaranteed to fire long before the match could ever complete on its own.
        AtomicBoolean keepPolling = new AtomicBoolean(true);
        MatcherWatchdog watchdog = MatcherWatchdog.newInstance(5, 5, System::currentTimeMillis, (delay, command) -> {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (keepPolling.get()) {
                    command.run();
                }
            });
            thread.start();
        });
        try {
            Grok.Parser parser = Grok.pattern(Source.EMPTY, "(?<a>a+)+b", watchdog);
            // match(String) treats a timeout as "no match" (see its javadoc); captures(String) is the
            // path that actually surfaces a timeout as an exception, same as production GROK execution.
            RuntimeException ex = expectThrows(RuntimeException.class, () -> parser.grok().captures("a".repeat(30) + "X"));
            assertThat(ex.getMessage(), containsString("interrupted"));
        } finally {
            keepPolling.set(false);
        }
    }

    public void testNoopWatchdogDoesNotInterruptSimplePattern() {
        Grok.Parser parser = Grok.pattern(Source.EMPTY, "%{IP:client_ip}", MatcherWatchdog.noop());
        assertNotNull(parser.grok().captures("192.168.1.1"));
    }
}
