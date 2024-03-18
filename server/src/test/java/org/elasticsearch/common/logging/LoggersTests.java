/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.Loggers.checkRestrictedLoggers;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LoggersTests extends ESTestCase {

    // Loggers.RESTRICTED_LOGGERS may be disabled by NetworkTraceFlag.TRACE_ENABLED, use internal API for testing
    private List<String> restrictedLoggers = List.of("org.apache.http", "com.amazonaws.request");

    public void testCheckRestrictedLoggers() {
        Settings settings;
        for (String restricted : restrictedLoggers) {
            for (String suffix : List.of("", ".xyz")) {
                String logger = restricted + suffix;
                for (Level level : List.of(Level.ALL, Level.TRACE, Level.DEBUG)) {
                    settings = Settings.builder().put("logger." + logger, level).build();
                    List<String> errors = checkRestrictedLoggers(settings, restrictedLoggers);
                    assertThat(errors, contains("Level [" + level + "] is not permitted for logger [" + logger + "]"));
                }
                for (Level level : List.of(Level.ERROR, Level.WARN, Level.INFO)) {
                    settings = Settings.builder().put("logger." + logger, level).build();
                    assertThat(checkRestrictedLoggers(settings, restrictedLoggers), hasSize(0));
                }

                settings = Settings.builder().put("logger." + logger, "INVALID").build();
                assertThat(checkRestrictedLoggers(settings, restrictedLoggers), hasSize(0));

                settings = Settings.builder().put("logger." + logger, (String) null).build();
                assertThat(checkRestrictedLoggers(settings, restrictedLoggers), hasSize(0));
            }
        }
    }

    public void testSetLevelWithRestrictions() {
        for (String restricted : restrictedLoggers) {
            TestLoggers.runWithLoggersRestored(() -> {
                // 'org.apache.http' is an example of a restricted logger,
                // a restricted component logger would be `org.apache.http.client.HttpClient` for instance,
                // and the parent logger is `org.apache`.
                Logger restrictedLogger = LogManager.getLogger(restricted);
                Logger restrictedComponent = LogManager.getLogger(restricted + ".component");
                Logger parentLogger = LogManager.getLogger(restricted.substring(0, restricted.lastIndexOf('.')));

                Loggers.setLevel(restrictedLogger, Level.INFO, restrictedLoggers);
                assertHasINFO(restrictedLogger, restrictedComponent);

                for (Logger log : List.of(restrictedComponent, restrictedLogger)) {
                    // DEBUG is rejected due to restriction
                    Loggers.setLevel(log, Level.DEBUG, restrictedLoggers);
                    assertHasINFO(restrictedComponent, restrictedLogger);
                }

                // OK for parent `org.apache`, but restriction is enforced for restricted descendants
                Loggers.setLevel(parentLogger, Level.DEBUG, restrictedLoggers);
                assertEquals(Level.DEBUG, parentLogger.getLevel());
                assertHasINFO(restrictedComponent, restrictedLogger);

                // Inheriting DEBUG of parent `org.apache` is rejected
                Loggers.setLevel(restrictedLogger, null, restrictedLoggers);
                assertHasINFO(restrictedComponent, restrictedLogger);

                // DEBUG of root logger isn't propagated to restricted loggers
                Loggers.setLevel(LogManager.getRootLogger(), Level.DEBUG, restrictedLoggers);
                assertEquals(Level.DEBUG, LogManager.getRootLogger().getLevel());
                assertHasINFO(restrictedComponent, restrictedLogger);
            });
        }
    }

    public void testStringSupplierAndFormatting() throws Exception {
        // adding a random id to allow test to run multiple times. See AbstractConfiguration#addAppender
        final MockAppender appender = new MockAppender("trace_appender" + randomInt());
        appender.start();
        final Logger testLogger = LogManager.getLogger(LoggersTests.class);
        Loggers.addAppender(testLogger, appender);
        Loggers.setLevel(testLogger, Level.TRACE);

        Throwable ex = randomException();
        testLogger.error(() -> format("an error message"), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.ERROR));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastMessage().getFormattedMessage(), equalTo("an error message"));

        ex = randomException();
        testLogger.warn(() -> format("a warn message: [%s]", "long gc"), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.WARN));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastMessage().getFormattedMessage(), equalTo("a warn message: [long gc]"));

        testLogger.info(() -> format("an info message a=[%s], b=[%s], c=[%s]", 1, 2, 3));
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.INFO));
        assertThat(appender.lastEvent.getThrown(), nullValue());
        assertThat(appender.lastMessage().getFormattedMessage(), equalTo("an info message a=[1], b=[2], c=[3]"));

        ex = randomException();
        testLogger.debug(() -> format("a debug message options = %s", asList("yes", "no")), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.DEBUG));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastMessage().getFormattedMessage(), equalTo("a debug message options = [yes, no]"));

        ex = randomException();
        testLogger.trace(() -> format("a trace message; element = [%s]", new Object[] { null }), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.TRACE));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastMessage().getFormattedMessage(), equalTo("a trace message; element = [null]"));
    }

    private Throwable randomException() {
        return randomFrom(
            new IOException("file not found"),
            new UnknownHostException("unknown hostname"),
            new OutOfMemoryError("out of space"),
            new IllegalArgumentException("index must be between 10 and 100")
        );
    }

    private static void assertHasINFO(Logger... loggers) {
        for (Logger log : loggers) {
            assertThat("Unexpected log level for [" + log.getName() + "]", log.getLevel(), is(Level.INFO));
        }
    }
}
