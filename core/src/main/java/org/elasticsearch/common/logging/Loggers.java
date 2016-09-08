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
import org.apache.logging.log4j.Logger;
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
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.util.CollectionUtils.asArrayList;

/**
 * A set of utilities around Logging.
 */
public class Loggers {

    static final String commonPrefix = System.getProperty("es.logger.prefix", "org.elasticsearch.");

    public static final String SPACE = " ";

    private static boolean consoleLoggingEnabled = true;

    public static void disableConsoleLogging() {
        consoleLoggingEnabled = false;
    }

    public static void enableConsoleLogging() {
        consoleLoggingEnabled = true;
    }

    public static boolean consoleLoggingEnabled() {
        return consoleLoggingEnabled;
    }

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
        return getLogger(buildClassLoggerName(clazz), settings, prefixes);
    }

    public static Logger getLogger(String loggerName, Settings settings, String... prefixes) {
        List<String> prefixesList = new ArrayList<>();
        if (Node.NODE_NAME_SETTING.exists(settings)) {
            prefixesList.add(Node.NODE_NAME_SETTING.get(settings));
        }
        if (prefixes != null && prefixes.length > 0) {
            prefixesList.addAll(asList(prefixes));
        }
        return getLogger(getLoggerName(loggerName), prefixesList.toArray(new String[prefixesList.size()]));
    }

    public static Logger getLogger(Logger parentLogger, String s) {
        return ESLoggerFactory.getLogger(parentLogger.<MessageFactory>getMessageFactory(), getLoggerName(parentLogger.getName() + s));
    }

    public static Logger getLogger(String s) {
        return ESLoggerFactory.getLogger(getLoggerName(s));
    }

    public static Logger getLogger(Class<?> clazz) {
        return ESLoggerFactory.getLogger(getLoggerName(buildClassLoggerName(clazz)));
    }

    public static Logger getLogger(Class<?> clazz, String... prefixes) {
        return getLogger(buildClassLoggerName(clazz), prefixes);
    }

    public static Logger getLogger(String name, String... prefixes) {
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
        return ESLoggerFactory.getLogger(prefix, getLoggerName(name));
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
        if (!"".equals(logger.getName())) {
            Configurator.setLevel(logger.getName(), level);
        } else {
            LoggerContext ctx = LoggerContext.getContext(false);
            Configuration config = ctx.getConfiguration();
            LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
            loggerConfig.setLevel(level);
            ctx.updateLoggers();
        }
    }

    private static String buildClassLoggerName(Class<?> clazz) {
        String name = clazz.getName();
        if (name.startsWith("org.elasticsearch.")) {
            name = Classes.getPackageName(clazz);
        }
        return name;
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch.")) {
            name = name.substring("org.elasticsearch.".length());
        }
        return commonPrefix + name;
    }

}
