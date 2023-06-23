/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test appender that can be used to verify that certain events were logged correctly
 */
public class MockLogAppender extends AbstractAppender {

    private final List<WrappedLoggingExpectation> expectations;

    public MockLogAppender() {
        super("mock", null, null, false, Property.EMPTY_ARRAY);
        /*
         * We use a copy-on-write array list since log messages could be appended while we are setting up expectations. When that occurs,
         * we would run into a concurrent modification exception from the iteration over the expectations in #append, concurrent with a
         * modification from #addExpectation.
         */
        expectations = new CopyOnWriteArrayList<>();
    }

    public void addExpectation(LoggingExpectation expectation) {
        expectations.add(new WrappedLoggingExpectation(expectation));
    }

    @Override
    public void append(LogEvent event) {
        for (LoggingExpectation expectation : expectations) {
            expectation.match(event);
        }
    }

    public void assertAllExpectationsMatched() {
        for (LoggingExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    public interface LoggingExpectation {
        void match(LogEvent event);

        void assertMatched();
    }

    public abstract static class AbstractEventExpectation implements LoggingExpectation {
        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String message;
        volatile boolean saw;

        public AbstractEventExpectation(String name, String logger, Level level, String message) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.message = message;
            this.saw = false;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger) && innerMatch(event)) {
                if (Regex.isSimpleMatchPattern(message)) {
                    if (Regex.simpleMatch(message, event.getMessage().getFormattedMessage())) {
                        saw = true;
                    }
                } else {
                    if (event.getMessage().getFormattedMessage().contains(message)) {
                        saw = true;
                    }
                }
            }
        }

        public boolean innerMatch(final LogEvent event) {
            return true;
        }

    }

    public static class UnseenEventExpectation extends AbstractEventExpectation {

        public UnseenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat("expected not to see " + name + " but did", saw, equalTo(false));
        }
    }

    public static class SeenEventExpectation extends AbstractEventExpectation {

        public SeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat("expected to see " + name + " but did not", saw, equalTo(true));
        }
    }

    public static class EventuallySeenEventExpectation extends SeenEventExpectation {

        private volatile boolean expectSeen = false;

        public EventuallySeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        public void setExpectSeen() {
            expectSeen = true;
        }

        @Override
        public void assertMatched() {
            if (expectSeen) {
                super.assertMatched();
            } else {
                assertThat("expected not to see " + name + " yet but did", saw, equalTo(false));
            }
        }
    }

    public static class ExceptionSeenEventExpectation extends SeenEventExpectation {

        private final Class<? extends Exception> clazz;
        private final String exceptionMessage;

        public ExceptionSeenEventExpectation(
            final String name,
            final String logger,
            final Level level,
            final String message,
            final Class<? extends Exception> clazz,
            final String exceptionMessage
        ) {
            super(name, logger, level, message);
            this.clazz = clazz;
            this.exceptionMessage = exceptionMessage;
        }

        @Override
        public boolean innerMatch(final LogEvent event) {
            return event.getThrown() != null
                && event.getThrown().getClass() == clazz
                && event.getThrown().getMessage().equals(exceptionMessage);
        }

    }

    public static class PatternSeenEventExpectation implements LoggingExpectation {

        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final Pattern pattern;
        volatile boolean saw;

        public PatternSeenEventExpectation(String name, String logger, Level level, String pattern) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.pattern = Pattern.compile(pattern);
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger)) {
                if (pattern.matcher(event.getMessage().getFormattedMessage()).matches()) {
                    saw = true;
                }
            }
        }

        @Override
        public void assertMatched() {
            assertThat(name, saw, equalTo(true));
        }

    }

    /**
     * A wrapper around {@link LoggingExpectation} to detect if the assertMatched method has been called
     */
    private static class WrappedLoggingExpectation implements LoggingExpectation {

        private final AtomicBoolean assertMatchedCalled = new AtomicBoolean(false);
        private final LoggingExpectation delegate;

        private WrappedLoggingExpectation(LoggingExpectation delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void match(LogEvent event) {
            delegate.match(event);
        }

        @Override
        public void assertMatched() {
            try {
                delegate.assertMatched();
            } finally {
                assertMatchedCalled.set(true);
            }
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    public Releasable capturing(Class<?>... classes) {
        start();
        final var loggers = Arrays.stream(classes).map(LogManager::getLogger).toArray(Logger[]::new);
        for (final var logger : loggers) {
            Loggers.addAppender(logger, this);
        }
        return () -> {
            for (final var logger : loggers) {
                Loggers.removeAppender(logger, this);
            }
            stop();
            // check that all expectations have been evaluated before this is released
            for (WrappedLoggingExpectation expectation : expectations) {
                assertThat(
                    "Method assertMatched() not called on LoggingExpectation instance before release: " + expectation,
                    expectation.assertMatchedCalled.get(),
                    is(true)
                );
            }
        };
    }

    /**
     * Executes an action and verifies expectations against the provided logger
     */
    public static void assertThatLogger(Runnable action, Class<?> loggerOwner, MockLogAppender.LoggingExpectation expectation) {
        MockLogAppender mockAppender = new MockLogAppender();
        try (var ignored = mockAppender.capturing(loggerOwner)) {
            mockAppender.addExpectation(expectation);
            action.run();
            mockAppender.assertAllExpectationsMatched();
        }
    }
}
