/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.elasticsearch.bootstrap.StartupException;
import org.elasticsearch.injection.guice.CreationException;
import org.elasticsearch.injection.guice.spi.Message;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class ConsoleThrowablePatternConverterTests extends ESTestCase {
    static final ConsoleThrowablePatternConverter converter = ConsoleThrowablePatternConverter.newInstance(null, null, true);

    String format(ConsoleThrowablePatternConverter converter, Throwable e) {
        LogEvent event = Log4jLogEvent.newBuilder().setThrown(e).build();
        var builder = new StringBuilder();
        converter.format(event, builder);
        return builder.toString();
    }

    String format(Throwable e) {
        return format(converter, e);
    }

    public void testNoException() {
        assertThat(format(null), emptyString());
    }

    public void testDisabledPassthrough() {
        // mimic no interactive console
        var converter = ConsoleThrowablePatternConverter.newInstance(null, null, false);
        var e = new StartupException(new RuntimeException("a cause"));
        // the cause of StartupException is not extracted by the parent pattern converter
        assertThat(format(converter, e), allOf(containsString("StartupException: "), containsString("RuntimeException: a cause")));
    }

    public void testStartupExceptionUnwrapped() {
        var e = new StartupException(new RuntimeException("an error"));
        assertThat(
            format(e),
            allOf(
                containsString("Elasticsearch failed to startup normally"),
                not(containsString("StartupException: ")),
                containsString("RuntimeException: ")
            )
        );
    }

    public void testCreationException() {
        var e = new CreationException(List.of(new Message("injection problem")));
        assertThat(format(e), containsString("There were problems initializing Guice"));
    }

    public void testStackTruncation() {
        var e = new NullPointerException();
        var stacktrace = e.getStackTrace();
        String output = format(e);
        String[] lines = output.split("\n");
        assertThat(lines.length, greaterThan(5));
        assertThat(lines[0], equalTo("java.lang.NullPointerException: null"));
        for (int i = 0; i < 5; ++i) {
            assertThat(lines[i + 1], equalTo("\tat " + stacktrace[i]));
        }
        assertThat(lines[6], emptyString());
    }

    public void testShortStack() {
        var e = new NullPointerException();
        StackTraceElement[] stacktrace = Arrays.copyOf(e.getStackTrace(), randomIntBetween(1, 4));
        e.setStackTrace(stacktrace);
        String output = format(e);
        String[] lines = output.split("\n");
        assertThat(lines.length, greaterThan(stacktrace.length + 2));
        assertThat(lines[0], equalTo("java.lang.NullPointerException: null"));
        for (int i = 0; i < stacktrace.length; ++i) {
            assertThat(lines[i + 1], equalTo("\tat " + stacktrace[i]));
        }
        assertThat(lines[stacktrace.length + 1], emptyString());
    }
}
