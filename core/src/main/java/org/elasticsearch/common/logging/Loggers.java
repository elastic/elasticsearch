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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.message.MessageFactory;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static javax.security.auth.login.Configuration.getConfiguration;
import static org.elasticsearch.common.util.CollectionUtils.asArrayList;

/**
 * A set of utilities around Logging.
 */
public class Loggers {

    public static final String SPACE = " ";

    public static Logger getLogger(Class<?> clazz, Settings settings, ShardId shardId, String... prefixes) {
        return getLogger(clazz, settings, shardId.getIndex(), asArrayList(Integer.toString(shardId.id()), prefixes).toArray(new String[0]));
    }

    /**
     * Just like {@link #getLogger(Class, org.elasticsearch.common.settings.Settings, ShardId, String...)} but String loggerName instead of
     * Class.
     */
    public static Logger getLogger(String loggerName, Settings settings, ShardId shardId, String... prefixes) {
        return getLogger(loggerName, settings,
            asArrayList(shardId.getIndexName(), Integer.toString(shardId.id()), prefixes).toArray(new String[0]));
    }

    public static Logger getLogger(Class<?> clazz, Settings settings, Index index, String... prefixes) {
        return getLogger(clazz, settings, asArrayList(SPACE, index.getName(), prefixes).toArray(new String[0]));
    }

    public static Logger getLogger(Class<?> clazz, Settings settings, String... prefixes) {
        final List<String> prefixesList = prefixesList(settings, prefixes);
        return getLogger(clazz, prefixesList.toArray(new String[prefixesList.size()]));
    }

    public static Logger getLogger(String loggerName, Settings settings, String... prefixes) {
        final List<String> prefixesList = prefixesList(settings, prefixes);
        return getLogger(loggerName, prefixesList.toArray(new String[prefixesList.size()]));
    }

    private static List<String> prefixesList(Settings settings, String... prefixes) {
        List<String> prefixesList = new ArrayList<>();
        if (Node.NODE_NAME_SETTING.exists(settings)) {
            prefixesList.add(Node.NODE_NAME_SETTING.get(settings));
        }
        if (prefixes != null && prefixes.length > 0) {
            prefixesList.addAll(asList(prefixes));
        }
        return prefixesList;
    }

    public static Logger getLogger(Logger parentLogger, String s) {
        assert parentLogger instanceof PrefixLogger;
        return ESLoggerFactory.getLogger(((PrefixLogger)parentLogger).prefix(), parentLogger.getName() + s);
    }

    public static Logger getLogger(String s) {
        return ESLoggerFactory.getLogger(s);
    }

    public static Logger getLogger(Class<?> clazz) {
        return ESLoggerFactory.getLogger(clazz);
    }

    public static Logger getLogger(Class<?> clazz, String... prefixes) {
        return ESLoggerFactory.getLogger(formatPrefix(prefixes), clazz);
    }

    public static Logger getLogger(String name, String... prefixes) {
        return ESLoggerFactory.getLogger(formatPrefix(prefixes), name);
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
                sb.append(" ");
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
        if (!LogManager.ROOT_LOGGER_NAME.equals(logger.getName())) {
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
        if (!logger.getName().equals(loggerConfig.getName())) {
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
        if (!logger.getName().equals(loggerConfig.getName())) {
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
