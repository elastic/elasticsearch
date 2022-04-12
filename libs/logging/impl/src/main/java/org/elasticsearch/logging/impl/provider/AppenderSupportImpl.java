/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.core.Appender;
import org.elasticsearch.logging.core.Filter;
import org.elasticsearch.logging.core.Layout;
import org.elasticsearch.logging.core.MockLogAppender;
import org.elasticsearch.logging.impl.ECSJsonLayout;
import org.elasticsearch.logging.impl.EcsLayoutImpl;
import org.elasticsearch.logging.impl.LogEventImpl;
import org.elasticsearch.logging.impl.Util;
import org.elasticsearch.logging.impl.testing.MockLogAppenderImpl;
import org.elasticsearch.logging.spi.AppenderSupport;

import java.util.List;

public class AppenderSupportImpl implements AppenderSupport {
    @Override
    public void addAppender(final org.elasticsearch.logging.Logger logger, final org.elasticsearch.logging.core.Appender appender) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        org.apache.logging.log4j.core.Appender appender1 = createLog4jAdapter(appender);
        appender1.start();

        config.addAppender(appender1);
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), Util.log4jLevel(logger.getLevel()), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.addAppender(appender1, null, null);
        ctx.updateLoggers();
    }

    @SuppressWarnings("unchecked")
    private static org.apache.logging.log4j.core.Appender createLog4jAdapter(org.elasticsearch.logging.core.Appender appender) {
        org.apache.logging.log4j.core.Filter filter = createLog4jFilter(appender.filter());
        Layout layout = appender.layout();
        return new AbstractAppender(appender.name(), filter, mapLayout(layout), false, Property.EMPTY_ARRAY) {

            @Override
            public void append(org.apache.logging.log4j.core.LogEvent event) {
                appender.append(new LogEventImpl(event));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static org.apache.logging.log4j.core.Layout<?> mapLayout(Layout layout) {
        return (org.apache.logging.log4j.core.Layout<?>) layout; // TODO PG sealed classes maybe...
    }

    private static org.apache.logging.log4j.core.Filter createLog4jFilter(org.elasticsearch.logging.core.Filter filter) {
        return new AbstractFilter() {
            @Override
            public org.apache.logging.log4j.core.Filter.Result filter(org.apache.logging.log4j.core.LogEvent event) {
                LogEventImpl logEvent = new LogEventImpl(event);
                Filter.Result result = filter.filter(logEvent);
                return mapResult(result);
            }
        };
    }

    public static org.apache.logging.log4j.core.Filter.Result mapResult(Filter.Result result) {
        return switch (result) {
            case ACCEPT -> org.apache.logging.log4j.core.Filter.Result.ACCEPT;
            case NEUTRAL -> org.apache.logging.log4j.core.Filter.Result.NEUTRAL;
            case DENY -> org.apache.logging.log4j.core.Filter.Result.DENY;
            default -> throw new IllegalStateException("Unexpected value: " + result);
        };
    }

    @Override
    public void addAppender(final Logger logger, final MockLogAppender appender) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        config.addAppender((org.apache.logging.log4j.core.Appender) appender.getLog4jAppender());
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), Util.log4jLevel(logger.getLevel()), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.addAppender((org.apache.logging.log4j.core.Appender) appender.getLog4jAppender(), null, null);
        ctx.updateLoggers();
    }

    @Override
    public void removeAppender(final Logger logger, final org.elasticsearch.logging.core.Appender appender) {
        removeAppender(logger, appender.name());
    }

    @Override
    public void removeAppender(final Logger logger, final MockLogAppender appender) {
        removeAppender(logger, "mock");
    }

    private static void removeAppender(Logger logger, String appenderName) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), Util.log4jLevel(logger.getLevel()), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.removeAppender(appenderName);
        ctx.updateLoggers();
    }

    @Override
    public Layout createECSLayout(String dataset) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();

        EcsLayout layout = ECSJsonLayout.newBuilder().setDataset(dataset).setConfiguration(config).build();

        return new EcsLayoutImpl(layout);
    }

    // @Override
    // public RateLimitingFilter createRateLimitingFilter() {
    // return new Log4jRateLimitingFilter();
    // }

    @Override
    public Appender createMockLogAppender(List<MockLogAppender.LoggingExpectation> expectations) throws IllegalAccessException {
        return new MockLogAppenderImpl(expectations);
    }
}
