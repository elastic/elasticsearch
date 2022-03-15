/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A set of utilities around Logging.
 */
public class Loggers {
    public Loggers() {}

    private static final String SPACE = " ";

    public static org.elasticsearch.logging.Logger getLogger(Class<?> clazz, int shardId, String... prefixes) {
        return getLogger(
            clazz,

            Stream.concat(Stream.of(Integer.toString(shardId)), Arrays.stream(prefixes)).toArray(String[]::new)
        );
    }

    // /**
    // * Just like {@link #getLogger(Class, ShardId, String...)} but String loggerName instead of
    // * Class and no extra prefixes. // TODO: fix docs
    // */
    public static org.elasticsearch.logging.Logger getLogger(String loggerName, String indexName, int shardId) {
        String prefix = formatPrefix(indexName, Integer.toString(shardId));
        return new LoggerImpl(new PrefixLogger(LogManager.getLogger(loggerName), prefix));
    }

    public static org.elasticsearch.logging.Logger getLoggerWithIndexName(Class<?> clazz, String indexName, String... prefixes) {
        return getLogger(clazz, Stream.concat(Stream.of(Loggers.SPACE, indexName), Arrays.stream(prefixes)).toArray(String[]::new));
    }

    public static org.elasticsearch.logging.Logger getLogger(Class<?> clazz, String... prefixes) {
        return new LoggerImpl(new PrefixLogger(LogManager.getLogger(clazz), formatPrefix(prefixes)));
    }

    public static org.elasticsearch.logging.Logger getLogger(org.elasticsearch.logging.Logger parentLogger, String s) {
        org.elasticsearch.logging.Logger inner = org.elasticsearch.logging.LogManager.getLogger(parentLogger.getName() + s);
        if (parentLogger instanceof PrefixLogger) {
            return new LoggerImpl(new PrefixLogger(Util.log4jLogger(inner), ((PrefixLogger) parentLogger).prefix()));
        }
        return inner;
    }

    public static org.elasticsearch.logging.Logger getLoggerImpl(Logger parentLogger, String s) {
        Logger inner = LogManager.getLogger(parentLogger.getName() + s);
        if (parentLogger instanceof PrefixLogger) {
            return new LoggerImpl(new PrefixLogger(inner, ((PrefixLogger) parentLogger).prefix()));
        }
        return new LoggerImpl(inner);
    }

    private static String formatPrefix(String... prefixes) {
        String prefix = null;
        if (prefixes != null && prefixes.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String prefixX : prefixes) {
                if (prefixX != null) {
                    if (prefixX.equals(SPACE)) {
                        sb.append(" ");
                    } else {
                        sb.append("[").append(prefixX).append("]");
                    }
                }
            }
            if (sb.length() > 0) {
                prefix = sb.toString();
            }
        }
        return prefix;
    }

    public static void setRootLoggerLevel(String level) {
        setLevelImpl(LogManager.getRootLogger(), level);
    }

    public static void setRootLoggerLevel(org.elasticsearch.logging.Level level) {
        setLevelImpl(LogManager.getRootLogger(), Util.log4jLevel(level));
    }

    /**
     * Set the level of the logger. If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    public static void setLevel(org.elasticsearch.logging.Logger logger, String level) {
        setLevelImpl(Util.log4jLogger(logger), level);
    }

    private static void setLevelImpl(Logger logger, String level) {
        final Level l;
        if (level == null) {
            l = null;
        } else {
            l = Level.valueOf(level);
        }
        setLevelImpl(logger, l);
    }

    public static void setLevel(org.elasticsearch.logging.Logger logger, org.elasticsearch.logging.Level level) {
        setLevelImpl(Util.log4jLogger(logger), Util.log4jLevel(level));
    }

    public static void setLevelImpl(Logger logger, Level level) {
        if (LogManager.ROOT_LOGGER_NAME.equals(logger.getName()) == false) {
            Configurator.setLevel(logger.getName(), level);
        } else {
            final LoggerContext ctx = LoggerContext.getContext(false);
            final Configuration config = ctx.getConfiguration();
            final LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
            loggerConfig.setLevel(level);
            ctx.updateLoggers();
        }

        // we have to descend the hierarchy
        final LoggerContext ctx = LoggerContext.getContext(false);
        for (final LoggerConfig loggerConfig : ctx.getConfiguration().getLoggers().values()) {
            if (LogManager.ROOT_LOGGER_NAME.equals(logger.getName()) || loggerConfig.getName().startsWith(logger.getName() + ".")) {
                Configurator.setLevel(loggerConfig.getName(), level);
            }
        }
    }

    public static void addAppender(final org.elasticsearch.logging.Logger logger, final Appender appender) {}

    public static void addAppender(final Logger logger, final Appender appender) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        config.addAppender(appender);
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), logger.getLevel(), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.addAppender(appender, null, null);
        ctx.updateLoggers();
    }

    public static void removeAppender(final Logger logger, final Appender appender) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), logger.getLevel(), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.removeAppender(appender.getName());
        ctx.updateLoggers();
    }

    static Appender findAppender(final Logger logger, final Class<? extends Appender> clazz) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        for (final Map.Entry<String, Appender> entry : loggerConfig.getAppenders().entrySet()) {
            if (entry.getValue().getClass().equals(clazz)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static void setLevel(Logger logger, org.elasticsearch.logging.Level info) {

    }
}
