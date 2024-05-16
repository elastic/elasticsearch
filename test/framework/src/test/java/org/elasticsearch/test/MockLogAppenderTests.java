/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockLogAppenderTests extends ESTestCase {

    public void testConcurrentLogAndLifecycle() throws Exception {
        Logger logger = LogManager.getLogger(MockLogAppenderTests.class);
        final var keepGoing = new AtomicBoolean(true);
        final var logThread = new Thread(() -> {
            while (keepGoing.get()) {
                logger.info("test");
            }
        });
        logThread.start();

        final var appender = new MockLogAppender();
        for (int i = 0; i < 1000; i++) {
            try (var ignored = appender.capturing(MockLogAppenderTests.class)) {
                Thread.yield();
            }
        }

        keepGoing.set(false);
        logThread.join();
    }
}
