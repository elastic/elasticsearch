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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A set of utilities around Logging.
 */
public class Loggers {

    public static final String SPACE = " ";

    public static final Setting<Level> LOG_DEFAULT_LEVEL_SETTING = new Setting<>(
        "logger.level",
        Level.INFO.name(),
        Level::valueOf,
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<Level> LOG_LEVEL_SETTING = Setting.prefixKeySetting(
        "logger.",
        (key) -> new Setting<>(key, Level.INFO.name(), Level::valueOf, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static Logger getLogger(Class<?> clazz, ShardId shardId, String... prefixes) {
        return getLogger(
            clazz,
            shardId.getIndex(),
            Stream.concat(Stream.of(Integer.toString(shardId.id())), Arrays.stream(prefixes)).toArray(String[]::new)
        );
    }

    /**
     * Just like {@link #getLogger(Class, ShardId, String...)} but String loggerName instead of
     * Class and no extra prefixes.
     */
    public static Logger getLogger(String loggerName, ShardId shardId) {
        String prefix = formatPrefix(shardId.getIndexName(), Integer.toString(shardId.id()));
        return new PrefixLogger(LogManager.getLogger(loggerName), prefix);
    }

    public static Logger getLogger(Class<?> clazz, Index index, String... prefixes) {
        return getLogger(clazz, Stream.concat(Stream.of(Loggers.SPACE, index.getName()), Arrays.stream(prefixes)).toArray(String[]::new));
    }

    public static Logger getLogger(Class<?> clazz, String... prefixes) {
        return new PrefixLogger(LogManager.getLogger(clazz), formatPrefix(prefixes));
    }

    public static Logger getLogger(Logger parentLogger, String s) {
        Logger inner = LogManager.getLogger(parentLogger.getName() + s);
        if (parentLogger instanceof PrefixLogger) {
            return new PrefixLogger(inner, ((PrefixLogger) parentLogger).prefix());
        }
        return inner;
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

    /**
     * Set the level of the logger. If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    public static void setLevel(Logger logger, String level) {
        final Level l;
        if (level == null) {
            l = null;
        } else {
            l = Level.valueOf(level);
        }
        setLevel(logger, l);
    }

    public static void setLevel(Logger logger, Level level) {
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

    public static Appender findAppender(final Logger logger, final Class<? extends Appender> clazz) {
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

}
