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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Test appender that can be used to verify that certain events were logged correctly
 */
public class MockLog implements Releasable {

    private static final Map<String, List<MockLog>> mockLogs = new ConcurrentHashMap<>();
    private static final MockAppender appender = new MockAppender();
    private final List<String> loggers;
    private final List<WrappedLoggingExpectation> expectations;
    private volatile boolean isAlive = true;

    @Override
    public void close() {
        isAlive = false;
        for (String logger : loggers) {
            mockLogs.compute(logger, (k, v) -> {
                assert v != null;
                v.remove(this);
                return v.isEmpty() ? null : v;
            });
        }
        // check that all expectations have been evaluated before this is released
        for (WrappedLoggingExpectation expectation : expectations) {
            assertThat(
                "Method assertMatched() not called on LoggingExpectation instance before release: " + expectation,
                expectation.assertMatchedCalled,
                is(true)
            );
        }
    }

    private static class MockAppender extends AbstractAppender {

        MockAppender() {
            super("mock", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            List<MockLog> appenders = mockLogs.get(event.getLoggerName());
            if (appenders == null) {
                // check if there is a root appender
                appenders = mockLogs.getOrDefault("", List.of());
            }
            for (MockLog appender : appenders) {
                if (appender.isAlive == false) {
                    continue;
                }
                for (LoggingExpectation expectation : appender.expectations) {
                    expectation.match(event);
                }
            }
        }
    }

    private MockLog(List<String> loggers) {
        /*
         * We use a copy-on-write array list since log messages could be appended while we are setting up expectations. When that occurs,
         * we would run into a concurrent modification exception from the iteration over the expectations in #append, concurrent with a
         * modification from #addExpectation.
         */
        expectations = new CopyOnWriteArrayList<>();
        this.loggers = loggers;
    }

    /**
     * Initialize the mock log appender with the log4j system.
     */
    public static void init() {
        appender.start();
        Loggers.addAppender(LogManager.getLogger(""), appender);
    }

    public void addExpectation(LoggingExpectation expectation) {
        expectations.add(new WrappedLoggingExpectation(expectation));
    }

    public void assertAllExpectationsMatched() {
        for (LoggingExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    public void awaitAllExpectationsMatched() {
        awaitAllExpectationsMatched(ESTestCase.SAFE_AWAIT_TIMEOUT);
    }

    // exposed for testing
    void awaitAllExpectationsMatched(TimeValue waitTime) {
        final var deadlineNanos = System.nanoTime() + waitTime.nanos();
        final var nanosPerMilli = TimeValue.timeValueMillis(1).nanos();
        try {
            for (LoggingExpectation expectation : expectations) {
                final var remainingMillis = (deadlineNanos - System.nanoTime() + nanosPerMilli - 1) / nanosPerMilli; // round up
                assertThat(remainingMillis, greaterThan(0L));
                expectation.awaitMatched(remainingMillis);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", interruptedException);
        }
    }

    /**
     * Keeps track of whether the {@link LogEvent} instances it receives match the expected content.
     */
    public interface LoggingExpectation {
        /**
         * Called on every {@link LogEvent} received by the captured appenders.
         */
        void match(LogEvent event);

        /**
         * Returns if this expectation is matched, otherwise throws an {@link AssertionError}.
         */
        void assertMatched();

        /**
         * Returns if this expectation is matched within the given number of milliseconds, otherwise throws an {@link AssertionError}.
         */
        default void awaitMatched(long millis) throws InterruptedException {
            assertMatched();
        }
    }

    public abstract static class AbstractEventExpectation implements LoggingExpectation {
        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String message;
        protected final CountDownLatch seenLatch = new CountDownLatch(1);

        public AbstractEventExpectation(String name, String logger, Level level, String message) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.message = message;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger) && innerMatch(event)) {
                if (Regex.isSimpleMatchPattern(message)) {
                    if (Regex.simpleMatch(message, event.getMessage().getFormattedMessage())) {
                        seenLatch.countDown();
                    }
                } else {
                    if (event.getMessage().getFormattedMessage().contains(message)) {
                        seenLatch.countDown();
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
            assertThat("expected not to see " + name + " but did", seenLatch.getCount(), equalTo(1L));
        }
    }

    public static class SeenEventExpectation extends AbstractEventExpectation {

        public SeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat("expected to see " + name + " but did not", seenLatch.getCount(), equalTo(0L));
        }

        @Override
        public void awaitMatched(long millis) throws InterruptedException {
            assertThat("expected to see " + name + " but did not", seenLatch.await(millis, TimeUnit.MILLISECONDS), equalTo(true));
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
                assertThat("expected not to see " + name + " yet but did", seenLatch.getCount(), equalTo(1L));
            }
        }

        @Override
        public void awaitMatched(long millis) throws InterruptedException {
            if (expectSeen) {
                super.awaitMatched(millis);
            } else {
                // do not wait for negative expectation
                assertThat("expected not to see " + name + " yet but did", seenLatch.getCount(), equalTo(1L));
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

        private final String name;
        private final String logger;
        private final Level level;
        private final Pattern pattern;
        private final CountDownLatch seenLatch = new CountDownLatch(1);

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
                    seenLatch.countDown();
                }
            }
        }

        @Override
        public void assertMatched() {
            assertThat(name, seenLatch.getCount(), equalTo(0L));
        }

        @Override
        public void awaitMatched(long millis) throws InterruptedException {
            assertThat(name, seenLatch.await(millis, TimeUnit.MILLISECONDS), equalTo(true));
        }
    }

    /**
     * A wrapper around {@link LoggingExpectation} to detect if the assertMatched method has been called
     */
    private static class WrappedLoggingExpectation implements LoggingExpectation {

        private volatile boolean assertMatchedCalled = false;
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
                assertMatchedCalled = true;
            }
        }

        @Override
        public void awaitMatched(long millis) throws InterruptedException {
            try {
                delegate.awaitMatched(millis);
            } finally {
                assertMatchedCalled = true;
            }
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    /**
     * Adds the list of class loggers to this {@link MockLog}.
     *
     * Stops and runs some checks on the {@link MockLog} once the returned object is released.
     */
    public static MockLog capture(Class<?>... classes) {
        return create(Arrays.stream(classes).map(Class::getCanonicalName).toList());
    }

    /**
     * Same as above except takes string class names of each logger.
     */
    public static MockLog capture(String... names) {
        return create(Arrays.asList(names));
    }

    private static MockLog create(List<String> loggers) {
        MockLog appender = new MockLog(loggers);
        addToMockLogs(appender, loggers);
        return appender;
    }

    private static void addToMockLogs(MockLog mockLog, List<String> loggers) {
        for (String logger : loggers) {
            mockLogs.compute(logger, (k, v) -> {
                if (v == null) {
                    v = new CopyOnWriteArrayList<>();
                }
                v.add(mockLog);
                return v;
            });
        }
    }

    /**
     * Executes an action and verifies expectations against the provided logger
     */
    public static void assertThatLogger(Runnable action, Class<?> loggerOwner, MockLog.LoggingExpectation... expectations) {
        try (var mockLog = MockLog.capture(loggerOwner)) {
            for (var expectation : expectations) {
                mockLog.addExpectation(expectation);
            }
            action.run();
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Executes an action and waits until the given logging expectations are satisfied.
     */
    public static void awaitLogger(Runnable action, Class<?> loggerOwner, MockLog.LoggingExpectation... expectations) {
        try (var mockLog = MockLog.capture(loggerOwner)) {
            for (var expectation : expectations) {
                mockLog.addExpectation(expectation);
            }
            action.run();
            mockLog.awaitAllExpectationsMatched();
        }
    }
}
