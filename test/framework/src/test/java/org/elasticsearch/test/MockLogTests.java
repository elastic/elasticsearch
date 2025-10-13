/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockLogTests extends ESTestCase {

    public void testConcurrentLogAndLifecycle() throws Exception {
        Logger logger = LogManager.getLogger(MockLogTests.class);
        final var keepGoing = new AtomicBoolean(true);
        final var logThread = new Thread(() -> {
            while (keepGoing.get()) {
                logger.info("test");
            }
        });
        logThread.start();

        for (int i = 0; i < 1000; i++) {
            try (var ignored = MockLog.capture(MockLogTests.class)) {
                Thread.yield();
            }
        }

        keepGoing.set(false);
        logThread.join();
    }

    @TestLogging(reason = "checking log behaviour", value = "org.elasticsearch.test.MockLogTests:INFO")
    public void testAwaitUnseenEvent() {
        try (var mockLog = MockLog.capture(MockLogTests.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("unseen", MockLogTests.class.getCanonicalName(), Level.INFO, "unexpected")
            );
            Thread.currentThread().interrupt(); // ensures no blocking calls
            mockLog.awaitAllExpectationsMatched();
            mockLog.assertAllExpectationsMatched();

            logger.info("unexpected");
            expectThrows(AssertionError.class, mockLog::awaitAllExpectationsMatched);
            expectThrows(AssertionError.class, mockLog::assertAllExpectationsMatched);

            assertTrue(Thread.interrupted()); // clear interrupt flag again
        }
    }

    @TestLogging(reason = "checking log behaviour", value = "org.elasticsearch.test.MockLogTests:INFO")
    public void testAwaitSeenEvent() throws InterruptedException {
        try (var mockLog = MockLog.capture(MockLogTests.class)) {
            mockLog.addExpectation(new MockLog.SeenEventExpectation("seen", MockLogTests.class.getCanonicalName(), Level.INFO, "expected"));

            expectThrows(AssertionError.class, () -> mockLog.awaitAllExpectationsMatched(TimeValue.timeValueMillis(10)));
            expectThrows(AssertionError.class, mockLog::assertAllExpectationsMatched);

            final var logThread = new Thread(() -> {
                logger.info("expected");
                mockLog.assertAllExpectationsMatched();
            });
            logThread.start();
            mockLog.awaitAllExpectationsMatched();
            mockLog.assertAllExpectationsMatched();
            logThread.join();
        }
    }

    @TestLogging(reason = "checking log behaviour", value = "org.elasticsearch.test.MockLogTests:INFO")
    public void testAwaitPatternEvent() throws InterruptedException {
        try (var mockLog = MockLog.capture(MockLogTests.class)) {
            mockLog.addExpectation(
                new MockLog.PatternSeenEventExpectation("seen", MockLogTests.class.getCanonicalName(), Level.INFO, ".*expected.*")
            );

            expectThrows(AssertionError.class, () -> mockLog.awaitAllExpectationsMatched(TimeValue.timeValueMillis(10)));
            expectThrows(AssertionError.class, mockLog::assertAllExpectationsMatched);

            final var logThread = new Thread(() -> {
                logger.info("blah blah expected blah blah");
                mockLog.assertAllExpectationsMatched();
            });
            logThread.start();
            mockLog.awaitAllExpectationsMatched();
            mockLog.assertAllExpectationsMatched();
            logThread.join();
        }
    }
}
