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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * A logger that logs deprecation notices. Logger should be initialized with a parent logger which name will be used
 * for deprecation logger. For instance <code>new DeprecationLogger("org.elasticsearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.elasticsearch.deprecation.test.SomeClass</code>. This allows to use
 * <code>deprecation</code> logger defined in log4j2.properties.
 *
 * Deprecation logs are written to deprecation log file - defined in log4j2.properties, as well as warnings added to a response header.
 * All deprecation usages are throttled basing on a key. Key is a string provided in an argument and can be prefixed with
 * <code>X-Opaque-Id</code>. This allows to throttle deprecations per client usage.
 * <code>deprecationLogger.deprecate("key","message {}", "param");</code>
 *
 * @see ThrottlingAndHeaderWarningLogger for throttling and header warnings implementation details
 */
public class DeprecationLogger {
    private final ThrottlingAndHeaderWarningLogger deprecationLogger;

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    private DeprecationLogger(Logger parentLogger) {
        deprecationLogger = new ThrottlingAndHeaderWarningLogger(parentLogger);
    }

    public static DeprecationLogger getLogger(Class<?> aClass) {
        return getLogger(toLoggerName(aClass));
    }

    public static DeprecationLogger getLogger(String name) {
        return new DeprecationLogger(deprecatedLoggerName(name));
    }

    private static Logger deprecatedLoggerName(String name) {
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

    public static void setThreadContext(ThreadContext threadContext) {
        HeaderWarning.setThreadContext(threadContext);
    }

    public static void removeThreadContext(ThreadContext threadContext) {
        HeaderWarning.removeThreadContext(threadContext);
    }

    /**
     * Logs a deprecation message, adding a formatted warning message as a response header on the thread context.
     * The deprecation message will be throttled to deprecation log.
     * method returns a builder as more methods are expected to be chained.
     */
    public DeprecationLoggerBuilder deprecate(final String key, final String msg, final Object... params) {
        return new DeprecationLoggerBuilder()
            .withDeprecation(key, msg, params);
    }

    public class DeprecationLoggerBuilder {

        public DeprecationLoggerBuilder withDeprecation(String key, String msg, Object[] params) {
            String opaqueId = HeaderWarning.getXOpaqueId();
            ESLogMessage deprecationMessage = DeprecatedMessage.of(opaqueId, msg, params);
            deprecationLogger.throttleLogAndAddWarning(key, deprecationMessage);
            return this;
        }

    }
}
