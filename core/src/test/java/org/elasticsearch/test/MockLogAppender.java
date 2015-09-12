/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test appender that can be used to verify that certain events were logged correctly
 */
public class MockLogAppender extends AppenderSkeleton {

    private final static String COMMON_PREFIX = System.getProperty("es.logger.prefix", "org.elasticsearch.");

    private List<LoggingExpectation> expectations;

    public MockLogAppender() {
        expectations = new ArrayList<>();
    }

    public void addExpectation(LoggingExpectation expectation) {
        expectations.add(expectation);
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        for (LoggingExpectation expectation : expectations) {
            expectation.match(loggingEvent);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public void assertAllExpectationsMatched() {
        for (LoggingExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    public interface LoggingExpectation {
        void match(LoggingEvent loggingEvent);

        void assertMatched();
    }

    public static abstract class AbstractEventExpectation implements LoggingExpectation {
        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String message;
        protected boolean saw;

        public AbstractEventExpectation(String name, String logger, Level level, String message) {
            this.name = name;
            this.logger = getLoggerName(logger);
            this.level = level;
            this.message = message;
            this.saw = false;
        }

        @Override
        public void match(LoggingEvent event) {
            if (event.getLevel() == level && event.getLoggerName().equals(logger)) {
                if (Regex.isSimpleMatchPattern(message)) {
                    if (Regex.simpleMatch(message, event.getMessage().toString())) {
                        saw = true;
                    }
                } else {
                    if (event.getMessage().toString().contains(message)) {
                        saw = true;
                    }
                }
            }
        }
    }

    public static class UnseenEventExpectation extends AbstractEventExpectation {

        public UnseenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat(name, saw, equalTo(false));
        }
    }

    public static class SeenEventExpectation extends AbstractEventExpectation {

        public SeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat(name, saw, equalTo(true));
        }
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch.")) {
            name = name.substring("org.elasticsearch.".length());
        }
        return COMMON_PREFIX + name;
    }
}
