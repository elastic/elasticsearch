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

/**
 * A logger that logs deprecation notices. Logger should be initialized with a parent logger which name will be used
 * for deprecation logger. For instance <code>DeprecationLogger.getLogger("org.elasticsearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.elasticsearch.deprecation.test.SomeClass</code>. This allows to use a
 * <code>deprecation</code> logger defined in log4j2.properties.
 * <p>
 * Logs are emitted at the custom {@link #DEPRECATION} level, and routed wherever they need to go using log4j. For example,
 * to disk using a rolling file appender, or added as a response header using {@link HeaderWarningAppender}.
 * <p>
 * Deprecation messages include a <code>key</code>, which is used for rate-limiting purposes. The log4j configuration
 * uses {@link RateLimitingFilter} to prevent the same message being logged repeatedly in a short span of time. This
 * key is combined with the <code>X-Opaque-Id</code> request header value, if supplied, which allows for per-client
 * message limiting.
 */
public class DeprecationLogger {

    /**
     * Deprecation messages are logged at this level.
     */
    public static Level DEPRECATION = Level.forName("DEPRECATION", Level.WARN.intLevel() + 1);

    private final Logger logger;

    /**
     * Creates a new deprecation logger for the supplied class. Internally, it delegates to
     * {@link #getLogger(String)}, passing the full class name.
     */
    public static DeprecationLogger getLogger(Class<?> aClass) {
        return getLogger(toLoggerName(aClass));
    }

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    public static DeprecationLogger getLogger(String name) {
        return new DeprecationLogger(name);
    }

    private DeprecationLogger(String parentLoggerName) {
        this.logger = LogManager.getLogger(getLoggerName(parentLoggerName));
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        return name;
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    /**
     * Logs a message at the {@link #DEPRECATION} level. The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger deprecate(
        final DeprecationCategory category,
        final String key,
        final String msg,
        final Object... params
    ) {
        assert category != DeprecationCategory.COMPATIBLE_API :
            "DeprecationCategory.COMPATIBLE_API should be logged with compatibleApiWarning method";
        ESLogMessage deprecationMessage = DeprecatedMessage.of(category, key, HeaderWarning.getXOpaqueId(), msg, params);
        logger.log(DEPRECATION, deprecationMessage);
        return this;
    }

    public DeprecationLogger compatibleApiWarning(
        final String key,
        final String msg,
        final Object... params) {
        String opaqueId = HeaderWarning.getXOpaqueId();
        ESLogMessage deprecationMessage = DeprecatedMessage.compatibleDeprecationMessage(key, opaqueId, msg, params);
        logger.log(DEPRECATION, deprecationMessage);
        return this;
    }

}
