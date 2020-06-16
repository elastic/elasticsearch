/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.threadpool.Scheduler;
import org.joni.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TimeoutCheckerTests extends FileStructureTestCase {

    private ScheduledExecutorService scheduler;

    @Before
    public void createScheduler() {
        scheduler = new Scheduler.SafeScheduledThreadPoolExecutor(1);
    }

    @After
    public void shutdownScheduler() {
        scheduler.shutdown();
    }

    public void testCheckNoTimeout() {
        NOOP_TIMEOUT_CHECKER.check("should never happen");
    }

    public void testCheckTimeoutNotExceeded() throws InterruptedException {
        TimeValue timeout = TimeValue.timeValueSeconds(10);
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("timeout not exceeded test", timeout, scheduler)) {
            for (int count = 0; count < 10; ++count) {
                timeoutChecker.check("should not timeout");
                Thread.sleep(randomIntBetween(1, 10));
            }
        }
    }

    public void testCheckTimeoutExceeded() throws Exception {
        TimeValue timeout = TimeValue.timeValueMillis(10);
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("timeout exceeded test", timeout, scheduler)) {
            assertBusy(() -> {
                ElasticsearchTimeoutException e = expectThrows(ElasticsearchTimeoutException.class,
                    () -> timeoutChecker.check("should timeout"));
                assertEquals("Aborting timeout exceeded test during [should timeout] as it has taken longer than the timeout of [" +
                    timeout + "]", e.getMessage());
            });
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/48861")
    public void testWatchdog() throws Exception {
        TimeValue timeout = TimeValue.timeValueMillis(500);
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("watchdog test", timeout, scheduler)) {
            TimeoutChecker.TimeoutCheckerWatchdog watchdog = (TimeoutChecker.TimeoutCheckerWatchdog) TimeoutChecker.watchdog;

            Matcher matcher = mock(Matcher.class);
            TimeoutChecker.watchdog.register(matcher);
            assertThat(watchdog.registry.get(Thread.currentThread()).matchers.size(), equalTo(1));
            try {
                assertBusy(() -> {
                    verify(matcher).interrupt();
                });
            } finally {
                TimeoutChecker.watchdog.unregister(matcher);
                assertThat(watchdog.registry.get(Thread.currentThread()).matchers.size(), equalTo(0));
            }
        }
    }

    public void testGrokCaptures() throws Exception {
        Grok grok = new Grok(Grok.getBuiltinPatterns(), "{%DATA:data}{%GREEDYDATA:greedydata}", TimeoutChecker.watchdog, logger::warn);
        TimeValue timeout = TimeValue.timeValueMillis(1);
        try (TimeoutChecker timeoutChecker = new TimeoutChecker("grok captures test", timeout, scheduler)) {

            assertBusy(() -> {
                ElasticsearchTimeoutException e = expectThrows(ElasticsearchTimeoutException.class,
                    () -> timeoutChecker.grokCaptures(grok, randomAlphaOfLength(1000000), "should timeout"));
                assertEquals("Aborting grok captures test during [should timeout] as it has taken longer than the timeout of [" +
                    timeout + "]", e.getMessage());
            });
        }
    }
}
