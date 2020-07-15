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
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;

/**
 * A logger that logs deprecation notices. Logger should be initialized with a parent logger which name will be used
 * for deprecation logger. For instance <code>new DeprecationLogger("org.elasticsearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.elasticsearch.deprecation.test.SomeClass</code>. This allows to use
 * <code>deprecation</code> logger defined in log4j2.properties.
 * <p>
 * Deprecation logs are written to deprecation log file - defined in log4j2.properties, as well as warnings added to a response header.
 * All deprecation usages are throttled basing on a key. Key is a string provided in an argument and can be prefixed with
 * <code>X-Opaque-Id</code>. This allows to throttle deprecations per client usage.
 * <code>deprecationLogger.deprecate("key","message {}", "param");</code>
 */
public class DeprecationLogger {
    public static Level DEPRECATION = Level.forName("DEPRECATION", Level.WARN.intLevel() + 1);

    // Only handle log events with the custom DEPRECATION level
    public static final LevelRangeFilter DEPRECATION_ONLY = LevelRangeFilter.createFilter(
        DEPRECATION,
        DEPRECATION,
        Filter.Result.ACCEPT,
        Filter.Result.DENY
    );

    private final Logger logger;

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    public DeprecationLogger(Logger parentLogger) {
        this.logger = parentLogger;
    }

    public static DeprecationLogger getLogger(Class<?> aClass) {
        return getLogger(toLoggerName(aClass));
    }

    public static DeprecationLogger getLogger(String name) {
        return new DeprecationLogger(getDeprecatedLoggerForName(name));
    }

    private static Logger getDeprecatedLoggerForName(String name) {
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        return LogManager.getLogger(name);
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    /**
     * Logs a deprecation message, adding a formatted warning message as a response header on the thread context.
     * The deprecation message will be throttled to deprecation log.
     * method returns a builder as more methods are expected to be chained.
     */
    public DeprecationLoggerBuilder deprecate(final String key, final String msg, final Object... params) {
        return new DeprecationLoggerBuilder().withDeprecation(key, msg, params);
    }

    public class DeprecationLoggerBuilder {

        public DeprecationLoggerBuilder withDeprecation(String key, String msg, Object[] params) {
            ESLogMessage deprecationMessage = DeprecatedMessage.of(HeaderWarning.getXOpaqueId(), msg, params).field("x-key", key);

            logger.log(DEPRECATION, deprecationMessage);

            return this;
        }
    }
}
