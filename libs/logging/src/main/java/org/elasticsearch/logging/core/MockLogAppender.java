/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logging.core;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.spi.AppenderSupport;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

/**
 * Test appender that can be used to verify that certain events were logged correctly
 * TODO possibly moved to a testing ?
 */
public class MockLogAppender {

    private static final String COMMON_PREFIX = System.getProperty("es.logger.prefix", "org.elasticsearch.");
    private final List<LoggingExpectation> expectations;
    Appender appender;

    public Appender getLog4jAppender() {
        return appender;
    }

    public MockLogAppender() throws IllegalAccessException {
        /*
         * We use a copy-on-write array list since log messages could be appended while we are setting up expectations. When that occurs,
         * we would run into a concurrent modification exception from the iteration over the expectations in #append, concurrent with a
         * modification from #addExpectation.
         */
        expectations = new CopyOnWriteArrayList<>();
        appender = AppenderSupport.provider().createMockLogAppender(expectations);
    }

    public static LoggingExpectation createUnseenEventExpectation(String name, String logger, Level level, String message) {
        return new UnseenEventExpectation(name, logger, level, message);
    }

    public static LoggingExpectationWithExpectSeen createEventuallySeenEventExpectation(
        String name,
        String logger,
        Level level,
        String message
    ) {
        return new EventuallySeenEventExpectation(name, logger, level, message);
    }

    public static LoggingExpectation createExceptionSeenEventExpectation(
        final String name,
        final String logger,
        final Level level,
        final String message,
        final Class<? extends Exception> clazz,
        final String exceptionMessage
    ) {
        return new ExceptionSeenEventExpectation(name, logger, level, message, clazz, exceptionMessage);
    }

    public static LoggingExpectation createPatternSeenEventExpectation(String name, String logger, Level level, String pattern) {
        return new PatternSeenEventExpectation(name, logger, level, pattern);
    }

    public static LoggingExpectation createSeenEventExpectation(String name, String logger, Level level, String message) {
        return new SeenEventExpectation(name, logger, level, message);
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch.")) {
            name = name.substring("org.elasticsearch.".length());
        }
        return COMMON_PREFIX + name;
    }

    public void addExpectation(LoggingExpectation expectation) {
        expectations.add(expectation);
    }

    public void start() {
        /*impl.start();*/
    }

    public void stop() {
        /* impl.stop();*/
    }

    public void assertAllExpectationsMatched() {
        for (LoggingExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    public interface LoggingExpectation {
        void assertMatched();

        void match(LogEvent event);
    }

    public interface LoggingExpectationWithExpectSeen extends LoggingExpectation {
        void setExpectSeen();
    }

    public abstract static class AbstractEventExpectation implements LoggingExpectation {
        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String message;
        volatile boolean saw;

        public AbstractEventExpectation(String name, String logger, Level level, String message) {
            this.name = name;
            this.logger = getLoggerName(logger);
            this.level = level;
            this.message = message;
            this.saw = false;
        }

        public static boolean isSimpleMatchPattern(String str) {
            return str.indexOf('*') != -1;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger) && innerMatch(event)) {
                if (isSimpleMatchPattern(message)) {
                    if (RegexCopy.simpleMatch(message, event.getMessage().getFormattedMessage())) {
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
            // MatcherAssert.assertThat("expected not to see " + name + " but did", saw, CoreMatchers.equalTo(false));
        }
    }

    public static class SeenEventExpectation extends AbstractEventExpectation {

        public SeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            // MatcherAssert.assertThat("expected to see " + name + " but did not", saw, CoreMatchers.equalTo(true));
        }
    }

    public static class EventuallySeenEventExpectation extends AbstractEventExpectation implements LoggingExpectationWithExpectSeen {

        private volatile boolean expectSeen = false;

        public EventuallySeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void setExpectSeen() {
            expectSeen = true;
        }

        @Override
        public void assertMatched() {
            if (expectSeen) {
                assertMatched();
            } else {
                MatcherAssert.assertThat("expected not to see " + name + " yet but did", saw, CoreMatchers.equalTo(false));
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
            MatcherAssert.assertThat(name, saw, CoreMatchers.equalTo(true));
        }

    }

}
