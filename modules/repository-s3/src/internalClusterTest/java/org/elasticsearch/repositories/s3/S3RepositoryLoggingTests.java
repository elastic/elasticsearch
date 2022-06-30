/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.empty;

public class S3RepositoryLoggingTests extends ESSingleNodeTestCase {

    private static class MockAppender extends AbstractAppender {

        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        protected MockAppender(String name, Filter filter) {
            super(name, Objects.requireNonNull(filter), PatternLayout.newBuilder().withPattern("%m").build(), false, new Property[0]);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        public List<LogEvent> getEvents() {
            return List.copyOf(events);
        }
    }

    private static MockAppender appender;

    @BeforeClass
    public static void setUpMockAppender() {
        appender = new MockAppender(getTestClass().getName(), new AbstractFilter() {
            @Override
            public Result filter(LogEvent event) {
                if (event.getLoggerName().startsWith("com.amazonaws.")) {
                    return Result.ACCEPT;
                }
                return Result.DENY;
            }
        });
        Loggers.addAppender(LogManager.getRootLogger(), appender);
        appender.start();
    }

    @AfterClass
    public static void tearDownMockAppender() {
        Loggers.removeAppender(LogManager.getRootLogger(), appender);
        appender.stop();
        appender = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(S3RepositoryPlugin.class);
    }

    public void testAwsSdkDoesNotEmitWarningDuringStaticInitialization() {
        List<LogEvent> events = appender.getEvents();
        assertThat("AWS SDK emits WARN logging messages at startup", events, empty());
    }
}
