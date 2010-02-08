/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.logging;

import com.google.common.collect.Lists;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.util.Classes;
import org.elasticsearch.util.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.google.common.collect.Lists.*;
import static java.util.Arrays.asList;

/**
 * A set of utilities around Logging.
 * <p/>
 * <p>The most important is the {@link #getLogger(Class)} which should be used instead of
 * {@link org.slf4j.LoggerFactory#getLogger(Class)}. It will use the package name as the
 * logging level without the actual class name.
 *
 * @author kimchy (Shay Banon)
 */
public class Loggers {

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

    public static Logger getLogger(Class clazz, Settings settings, ShardId shardId, String... prefixes) {
        return getLogger(clazz, settings, shardId.index(), Lists.asList(Integer.toString(shardId.id()), prefixes).toArray(new String[0]));
    }

    public static Logger getLogger(Class clazz, Settings settings, Index index, String... prefixes) {
        return getLogger(clazz, settings, Lists.asList(index.name(), prefixes).toArray(new String[0]));
    }

    public static Logger getLogger(Class clazz, Settings settings, String... prefixes) {
        List<String> prefixesList = newArrayList();
        if (settings.getAsBoolean("logger.logHostAddress", false)) {
            try {
                prefixesList.add(InetAddress.getLocalHost().getHostAddress());
            } catch (UnknownHostException e) {
                // ignore
            }
        }
        if (settings.getAsBoolean("logger.logHostName", false)) {
            try {
                prefixesList.add(InetAddress.getLocalHost().getHostName());
            } catch (UnknownHostException e) {
                // ignore
            }
        }
        String name = settings.get("name");
        if (name != null) {
            prefixesList.add(name);
        }
        if (prefixes != null && prefixes.length > 0) {
            prefixesList.addAll(asList(prefixes));
        }
        return getLogger(clazz, prefixesList.toArray(new String[prefixesList.size()]));
    }

    public static Logger getLogger(Logger parentLogger, String s) {
        Logger logger = getLogger(parentLogger.getName() + s);
        if (parentLogger instanceof PrefixLoggerAdapter) {
            return new PrefixLoggerAdapter(((PrefixLoggerAdapter) parentLogger).prefix(), logger);
        }
        return logger;
    }

    public static Logger getLogger(String s) {
        return LoggerFactory.getLogger(s);
    }

    public static Logger getLogger(Class clazz) {
        return LoggerFactory.getLogger(getLoggerName(clazz));
    }

    public static Logger getLogger(Class clazz, String... prefixes) {
        return getLogger(LoggerFactory.getLogger(getLoggerName(clazz)), prefixes);
    }

    public static Logger getLogger(Logger logger, String... prefixes) {
        if (prefixes == null || prefixes.length == 0) {
            return logger;
        }
        StringBuilder sb = new StringBuilder();
        for (String prefix : prefixes) {
            if (prefix != null) {
                sb.append("[").append(prefix).append("]");
            }
        }
        if (sb.length() == 0) {
            return logger;
        }
        sb.append(" ");
        return new PrefixLoggerAdapter(sb.toString(), logger);
    }

    private static String getLoggerName(Class clazz) {
        String name = clazz.getName();
        if (name.startsWith("org.elasticsearch.")) {
            name = Classes.getPackageName(clazz);
        }
        return getLoggerName(name);
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch.")) {
            return name.substring("org.elasticsearch.".length());
        }
        return name;
    }
}
