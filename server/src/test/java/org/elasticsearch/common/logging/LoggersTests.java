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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.UnknownHostException;

import static java.util.Arrays.asList;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LoggersTests extends ESTestCase {

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
}
