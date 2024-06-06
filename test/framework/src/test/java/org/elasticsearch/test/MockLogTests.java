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
            try (var mockLog = MockLog.capture(MockLogTests.class)) {
                Thread.yield();
            }
        }

        keepGoing.set(false);
        logThread.join();
    }
}
