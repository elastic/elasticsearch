/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.List;

/**
 * Logger that captures events and appends them to in memory lists, with one
 * list for each log level. This works with the global log manager context,
 * meaning that there could only be a single logger with the same name.
 */
public class CapturingLogger {

    /**
     * Constructs a new {@link CapturingLogger} named as the fully qualified name of
     * the invoking method. One name can be assigned to a single logger globally, so
     * don't call this method multiple times in the same method.
     *
     * @param level
     *            The minimum priority level of events that will be captured.
     * @param layout
     *            Optional parameter allowing to set the layout format of events.
     *            This is useful because events are captured to be inspected (and
     *            parsed) later. When parsing, it is useful to be in control of the
     *            printing format as well. If not specified,
     *            {@code event.getMessage().getFormattedMessage()} is called to
     *            format the event.
     * @return The new logger.
     */
    public static Logger newCapturingLogger(final Level level, @Nullable StringLayout layout) throws IllegalAccessException {
        // careful, don't "bury" this on the call stack, unless you know what you're doing
        final StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
        final String name = caller.getClassName() + "." + caller.getMethodName() + "." + level.toString();
        final Logger logger = LogManager.getLogger(name);
        Loggers.setLevel(logger, level);
        final MockAppender appender = new MockAppender(name, layout);
        appender.start();
        Loggers.addAppender(logger, appender);
        return logger;
    }

    private static MockAppender getMockAppender(final String name) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(name);
        return (MockAppender) loggerConfig.getAppenders().get(name);
    }

    /**
     * Checks if the logger's appender has captured any events.
     *
     * @param name
     *            The unique global name of the logger.
     * @return {@code true} if no event has been captured, {@code false} otherwise.
     */
    public static boolean isEmpty(final String name) {
        final MockAppender appender = getMockAppender(name);
        return appender.isEmpty();
    }

    /**
     * Gets the captured events for a logger by its name.
     *
     * @param name
     *            The unique global name of the logger.
     * @param level
     *            The priority level of the captured events to be returned.
     * @return A list of captured events formated to {@code String}.
     */
    public static List<String> output(final String name, final Level level) {
        final MockAppender appender = getMockAppender(name);
        return appender.output(level);
    }

    private static class MockAppender extends AbstractAppender {

        public final List<String> error = new ArrayList<>();
        public final List<String> warn = new ArrayList<>();
        public final List<String> info = new ArrayList<>();
        public final List<String> debug = new ArrayList<>();
        public final List<String> trace = new ArrayList<>();

        private MockAppender(final String name, StringLayout layout) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), layout);
        }

        @Override
        public void append(LogEvent event) {
            switch (event.getLevel().toString()) {
                // we can not keep a reference to the event here because Log4j is using a thread
                // local instance under the hood
                case "ERROR":
                    error.add(formatMessage(event));
                    break;
                case "WARN":
                    warn.add(formatMessage(event));
                    break;
                case "INFO":
                    info.add(formatMessage(event));
                    break;
                case "DEBUG":
                    debug.add(formatMessage(event));
                    break;
                case "TRACE":
                    trace.add(formatMessage(event));
                    break;
                default:
                    throw invalidLevelException(event.getLevel());
            }
        }

        private String formatMessage(LogEvent event) {
            final Layout<?> layout = getLayout();
            if (layout instanceof StringLayout) {
                return ((StringLayout) layout).toSerializable(event);
            } else {
                return event.getMessage().getFormattedMessage();
            }
        }

        private IllegalArgumentException invalidLevelException(Level level) {
            return new IllegalArgumentException("invalid level, expected [ERROR|WARN|INFO|DEBUG|TRACE] but was [" + level + "]");
        }

        public boolean isEmpty() {
            return error.isEmpty() && warn.isEmpty() && info.isEmpty() && debug.isEmpty() && trace.isEmpty();
        }

        public List<String> output(Level level) {
            switch (level.toString()) {
                case "ERROR":
                    return error;
                case "WARN":
                    return warn;
                case "INFO":
                    return info;
                case "DEBUG":
                    return debug;
                case "TRACE":
                    return trace;
                default:
                    throw invalidLevelException(level);
            }
        }
    }

}
