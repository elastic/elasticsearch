/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class TimeoutCheckerTests extends FileStructureTestCase {

    private ScheduledExecutorService scheduler;

    @Before
    public void createScheduler() {
        scheduler = new ScheduledThreadPoolExecutor(1);
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
}
