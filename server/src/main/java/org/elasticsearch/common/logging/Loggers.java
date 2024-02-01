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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A set of utilities around Logging.
 */
public class Loggers {

    public static final String SPACE = " ";

    /** Restricted loggers can't be set to a level less specific than INFO. */
    private static final List<String> RESTRICTED_LOGGERS = List.of("org.apache.http");

    public static final Setting<Level> LOG_DEFAULT_LEVEL_SETTING = new Setting<>(
        "logger.level",
        Level.INFO.name(),
        Level::valueOf,
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<Level> LOG_LEVEL_SETTING = Setting.prefixKeySetting(
        "logger.",
        (key) -> new Setting<>(key, Level.INFO.name(), value -> {
            Level level = Level.valueOf(value);
            if (level.isMoreSpecificThan(Level.INFO) == false) {
                String logger = key.substring("logger.".length());
                if (RESTRICTED_LOGGERS.stream().anyMatch(r -> isSameOrDescendantOf(logger, r))) {
                    throw new IllegalArgumentException("Level [" + value + "] not permitted for logger [" + logger + "].");
                }
            }
            return level;
        }, Setting.Property.Dynamic, Setting.Property.NodeScope)
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
        setLevel(logger, level == null ? null : Level.valueOf(level), List.of());
    }

    public static void setLevel(Logger logger, Level level) {
        setLevel(logger, level, List.of());
    }

    public static void setRestrictionAwareLevel(Logger logger, String level) {
        setLevel(logger, level == null ? null : Level.valueOf(level), RESTRICTED_LOGGERS);
    }

    public static void setRestrictionAwareLevel(Logger logger, Level level) {
        setLevel(logger, level, RESTRICTED_LOGGERS);
    }

    // visible for testing
    protected static void setLevel(Logger logger, Level level, List<String> restrictedLoggers) {
        // If configuring an ancestor / root, the restriction has to be explicitly set afterward.
        boolean setRestriction = false;

        if (isRootLogger(logger) == false) {
            Level actual = level != null ? level : parentLoggerLevel(logger);
            if (actual.isMoreSpecificThan(Level.INFO) == false) {
                for (String restricted : restrictedLoggers) {
                    if (isSameOrDescendantOf(logger.getName(), restricted)) {
                        LogManager.getLogger(Loggers.class).warn("Log level of [{}] can't be less specific than INFO", logger.getName());
                        return;
                    }
                    if (isDescendantOf(restricted, logger.getName())) {
                        setRestriction = true;
                    }
                }
            }
            Configurator.setLevel(logger.getName(), level);
        } else {
            final LoggerContext ctx = LoggerContext.getContext(false);
            final Configuration config = ctx.getConfiguration();
            final LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
            loggerConfig.setLevel(level);
            ctx.updateLoggers();

            Level actual = level != null ? level : LogManager.getRootLogger().getLevel();
            if (actual.isMoreSpecificThan(Level.INFO) == false) {
                setRestriction = true;
            }
        }

        if (setRestriction) {
            restrictedLoggers.stream()
                .filter(restricted -> isRootLogger(logger) || isDescendantOf(restricted, logger.getName()))
                .forEach(restricted -> setLevel(LogManager.getLogger(restricted), Level.INFO, List.of()));
        }

        // we have to descend the hierarchy
        final LoggerContext ctx = LoggerContext.getContext(false);
        for (final LoggerConfig loggerConfig : ctx.getConfiguration().getLoggers().values()) {
            // make sure to not overwrite the log level of restricted loggers
            boolean isRestricted = setRestriction
                && restrictedLoggers.stream().anyMatch(restricted -> isSameOrDescendantOf(loggerConfig.getName(), restricted));
            if (isRestricted == false && isDescendantOf(loggerConfig.getName(), logger.getName())) {
                Configurator.setLevel(loggerConfig.getName(), level);
            }
        }
    }

    private static Level parentLoggerLevel(Logger logger) {
        int idx = logger.getName().lastIndexOf('.');
        if (idx != -1) {
            return LogManager.getLogger(logger.getName().substring(0, idx)).getLevel();
        }
        return LogManager.getRootLogger().getLevel();
    }

    private static boolean isRootLogger(Logger logger) {
        return LogManager.ROOT_LOGGER_NAME.equals(logger.getName());
    }

    private static boolean isDescendantOf(String candidate, String ancestor) {
        return candidate.startsWith(ancestor + ".");
    }

    private static boolean isSameOrDescendantOf(String candidate, String ancestor) {
        return candidate.equals(ancestor) || isDescendantOf(candidate, ancestor);
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
