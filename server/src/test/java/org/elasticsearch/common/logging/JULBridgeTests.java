/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.MockLog.LoggingExpectation;
import org.elasticsearch.test.MockLog.SeenEventExpectation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;

public class JULBridgeTests extends ESTestCase {

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
    private static java.util.logging.Level savedLevel;
    private static Handler[] savedHandlers;

    @BeforeClass
    public static void saveLoggerState() {
        savedLevel = logger.getLevel();
        savedHandlers = logger.getHandlers();
    }

    @Before
    public void resetLogger() {
        logger.setLevel(java.util.logging.Level.ALL);
        for (var existingHandler : logger.getHandlers()) {
            logger.removeHandler(existingHandler);
        }
    }

    @AfterClass
    public static void restoreLoggerState() {
        logger.setLevel(savedLevel);
        for (var existingHandler : logger.getHandlers()) {
            logger.removeHandler(existingHandler);
        }
        for (var savedHandler : savedHandlers) {
            logger.addHandler(savedHandler);
        }
    }

    private void assertLogged(Runnable loggingCode, LoggingExpectation... expectations) {
        Logger testLogger = LogManager.getLogger("");
        Level savedLevel = testLogger.getLevel();

        try (var mockLog = MockLog.capture("")) {
            Loggers.setLevel(testLogger, Level.ALL);
            for (var expectation : expectations) {
                mockLog.addExpectation(expectation);
            }
            loggingCode.run();
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(testLogger, savedLevel);
        }
    }

    private void assertMessage(String msg, java.util.logging.Level julLevel, Level expectedLevel) {
        assertLogged(() -> logger.log(julLevel, msg), new SeenEventExpectation(msg, "", expectedLevel, msg));
    }

    private static java.util.logging.Level julLevel(int value) {
        return java.util.logging.Level.parse(Integer.toString(value));
    }

    public void testInstallRemovesExistingHandlers() {
        logger.addHandler(new ConsoleHandler());
        JULBridge.install();
        assertThat(logger.getHandlers(), arrayContaining(instanceOf(JULBridge.class)));
    }

    public void testKnownLevels() {
        JULBridge.install();
        assertMessage("off msg", java.util.logging.Level.OFF, Level.OFF);
        assertMessage("severe msg", java.util.logging.Level.SEVERE, Level.ERROR);
        assertMessage("warning msg", java.util.logging.Level.WARNING, Level.WARN);
        assertMessage("info msg", java.util.logging.Level.INFO, Level.INFO);
        assertMessage("fine msg", java.util.logging.Level.FINE, Level.DEBUG);
        assertMessage("finest msg", java.util.logging.Level.FINEST, Level.TRACE);
    }

    public void testCustomLevels() {
        JULBridge.install();
        assertMessage("smallest level", julLevel(Integer.MIN_VALUE), Level.ALL);
        assertMessage("largest level", julLevel(Integer.MAX_VALUE), Level.OFF);
        assertMessage("above severe", julLevel(java.util.logging.Level.SEVERE.intValue() + 1), Level.ERROR);
        assertMessage("above warning", julLevel(java.util.logging.Level.WARNING.intValue() + 1), Level.WARN);
        assertMessage("above info", julLevel(java.util.logging.Level.INFO.intValue() + 1), Level.INFO);
        assertMessage("above fine", julLevel(java.util.logging.Level.FINE.intValue() + 1), Level.DEBUG);
        assertMessage("above finest", julLevel(java.util.logging.Level.FINEST.intValue() + 1), Level.TRACE);
    }

    public void testThrowable() {
        JULBridge.install();
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
        assertLogged(() -> logger.log(java.util.logging.Level.SEVERE, "error msg", new Exception("some error")), new LoggingExpectation() {
            boolean matched = false;

            @Override
            public void match(LogEvent event) {
                Throwable thrown = event.getThrown();
                matched = event.getLoggerName().equals("")
                    && event.getMessage().getFormattedMessage().equals("error msg")
                    && thrown != null
                    && thrown.getMessage().equals("some error");
            }

            @Override
            public void assertMatched() {
                assertThat("expected to see error message but did not", matched, equalTo(true));
            }
        });
    }

    public void testChildLogger() {
        JULBridge.install();
        java.util.logging.Logger childLogger = java.util.logging.Logger.getLogger("foo");
        assertLogged(() -> childLogger.info("child msg"), new SeenEventExpectation("child msg", "foo", Level.INFO, "child msg"));
    }

    public void testNullMessage() {
        JULBridge.install();
        assertLogged(() -> logger.info((String) null), new SeenEventExpectation("null msg", "", Level.INFO, "<null message>"));
    }

    public void testFormattedMessage() {
        JULBridge.install();
        assertLogged(
            () -> logger.log(java.util.logging.Level.INFO, "{0}", "a var"),
            new SeenEventExpectation("formatted msg", "", Level.INFO, "a var")
        );
    }
}
