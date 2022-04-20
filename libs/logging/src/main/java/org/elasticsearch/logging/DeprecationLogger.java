/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.message.ESMapMessage;
import org.elasticsearch.logging.message.Message;
import org.elasticsearch.logging.spi.MessageFactory;
import org.elasticsearch.logging.spi.ServerSupport;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * A logger that logs deprecation notices. Logger should be initialized with a class or name which will be used
 * for deprecation logger. For instance <code>DeprecationLogger.getLogger("org.elasticsearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.elasticsearch.deprecation.test.SomeClass</code>. This allows to use a
 * <code>deprecation</code> logger defined in log4j2.properties.
 * <p>
 * Logs are emitted at the custom {@link #CRITICAL} level, and routed wherever they need to go using log4j. For example,
 * to disk using a rolling file appender, or added as a response header using {x@link HeaderWarningAppender}. //TODO PG
 * <p>
 * Deprecation messages include a <code>key</code>, which is used for rate-limiting purposes. The log4j configuration
 * uses {x@link RateLimitingFilter}//TODO PG to prevent the same message being logged repeatedly in a short span of time. This
 * key is combined with the <code>X-Opaque-Id</code> request header value, if supplied, which allows for per-client
 * message limiting.
 */
// TODO: PG i wonder if we coudl expose an interface and inject this implementation? the same we would do for a regular Logger interface
public final class DeprecationLogger {
    public static final String ELASTIC_ORIGIN_FIELD_NAME = "elasticsearch.elastic_product_origin";
    public static final String KEY_FIELD_NAME = "event.code";
    public static final String X_OPAQUE_ID_FIELD_NAME = "elasticsearch.http.request.x_opaque_id";
    public static final String ECS_VERSION = "1.2.0";
    /**
     * Deprecation messages are logged at this level.
     * More serious that WARN by 1, but less serious than ERROR
     */
    public static Level CRITICAL = Level.of("CRITICAL", Level.WARN.getSeverity() - 1);

    private static volatile List<String> skipTheseDeprecations = Collections.emptyList();

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

    /**
     * The DeprecationLogger uses the "deprecation.skip_deprecated_settings" setting to decide whether to log a deprecation for a setting.
     * This is a node setting. This method initializes the DeprecationLogger class with the node settings for the node in order to read the
     * "deprecation.skip_deprecated_settings" setting. This only needs to be called once per JVM. If it is not called, the default behavior
     * is to assume that the "deprecation.skip_deprecated_settings" setting is not set.
     *
     * @param nodeSkipDeprecatedSetting The settings for this node  // TODO: typy this up
     */
    public static void initialize(List<String> nodeSkipDeprecatedSetting) {
        skipTheseDeprecations = nodeSkipDeprecatedSetting == null ? Collections.emptyList() : nodeSkipDeprecatedSetting;
        // nodeSettings.getAsList("deprecation.skip_deprecated_settings");
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
     * Logs a message at the {@link DeprecationLogger#CRITICAL} level.
     * This log will indicate that a change will break in next version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger critical(final DeprecationCategory category, final String key, final String msg, final Object... params) {
        return logDeprecation(CRITICAL, category, key, msg, params);
    }

    /**
     * Logs a message at the {@link Level#WARN} level for less critical deprecations
     * that won't break in next version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger warn(final DeprecationCategory category, final String key, final String msg, final Object... params) {
        return logDeprecation(Level.WARN, category, key, msg, params);
    }

    private DeprecationLogger logDeprecation(Level level, DeprecationCategory category, String key, String msg, Object[] params) {
        assert category != DeprecationCategory.COMPATIBLE_API
            : "DeprecationCategory.COMPATIBLE_API should be logged with compatibleApiWarning method";
        String opaqueId = ServerSupport.INSTANCE.getXOpaqueIdHeader();
        String productOrigin = ServerSupport.INSTANCE.getProductOriginHeader();
        Message deprecationMessage = DeprecatedMessage.of(category, key, opaqueId, productOrigin, msg, params);
        doPrivilegedLog(level, deprecationMessage);
        return this;
    }

    private void doPrivilegedLog(Level level, Message deprecationMessage) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            logger.log(level, deprecationMessage);
            return null;
        });
    }

    /**
     * Used for handling previous version RestApiCompatible logic.
     * Logs a message at the {@link DeprecationLogger#CRITICAL} level
     * that have been broken in previous version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger compatibleCritical(final String key, final String msg, final Object... params) {
        return compatible(CRITICAL, key, msg, params);
    }

    /**
     * Used for handling previous version RestApiCompatible logic.
     * Logs a message at the given level
     * that has been broken in previous version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */

    public DeprecationLogger compatible(final Level level, final String key, final String msg, final Object... params) {
        String opaqueId = ServerSupport.INSTANCE.getXOpaqueIdHeader();
        String productOrigin = ServerSupport.INSTANCE.getProductOriginHeader();
        Message deprecationMessage = DeprecatedMessage.compatibleDeprecationMessage(key, opaqueId, productOrigin, msg, params);
        logger.log(level, deprecationMessage);
        return this;
    }

    /**
     * Deprecation log messages are categorised so that consumers of the logs can easily aggregate them.
     * <p>
     * When categorising a message, you should consider the impact of the work required to mitigate the
     * deprecation. For example, a settings change would normally be categorised as {@link #SETTINGS},
     * but if the setting in question was related to security configuration, it may be more appropriate
     * to categorise the deprecation message as {@link #SECURITY}.
     */
    public enum DeprecationCategory {
        AGGREGATIONS,
        ANALYSIS,
        API,
        COMPATIBLE_API,
        INDICES,
        MAPPINGS,
        OTHER,
        PARSING,
        PLUGINS,
        QUERIES,
        SCRIPTING,
        SECURITY,
        SETTINGS,
        TEMPLATES
    }

    /**
     * A logger message used by {@link DeprecationLogger}, enriched with fields
     * named following ECS conventions. Carries x-opaque-id field if provided in the headers.
     * Will populate the x-opaque-id field in JSON logs.
     */
    // TODO: PG I would prefer to hide it, package private??
    static final class DeprecatedMessage {

        static final MessageFactory provider = MessageFactory.provider();

        private DeprecatedMessage() {}

        // @SuppressLoggerChecks(reason = "safely delegates to logger")
        public static Message of(
            DeprecationCategory category,
            String key,
            String xOpaqueId,
            String productOrigin,
            String messagePattern,
            Object... args
        ) {
            return getEsLogMessage(category, key, xOpaqueId, productOrigin, messagePattern, args);
        }

        // @SuppressLoggerChecks(reason = "safely delegates to logger")
        public static Message compatibleDeprecationMessage(
            String key,
            String xOpaqueId,
            String productOrigin,
            String messagePattern,
            Object... args
        ) {
            return getEsLogMessage(DeprecationCategory.COMPATIBLE_API, key, xOpaqueId, productOrigin, messagePattern, args);
        }

        // @SuppressLoggerChecks(reason = "safely delegates to logger")
        private static Message getEsLogMessage(
            DeprecationCategory category,
            String key,
            String xOpaqueId,
            String productOrigin,
            String messagePattern,
            Object[] args
        ) {
            ESMapMessage esLogMessage = provider.createMapMessage(messagePattern, args)
                .field("data_stream.dataset", "deprecation.elasticsearch")
                .field("data_stream.type", "logs")
                .field("data_stream.namespace", "default")
                .field(KEY_FIELD_NAME, key)
                .field("elasticsearch.event.category", category.name().toLowerCase(Locale.ROOT));

            if (isNullOrEmpty(xOpaqueId)) {
                return esLogMessage;
            }

            return esLogMessage.field(X_OPAQUE_ID_FIELD_NAME, xOpaqueId).field(ELASTIC_ORIGIN_FIELD_NAME, productOrigin);
        }

        // TODO: move to core Strings?
        public static boolean isNullOrEmpty(CharSequence str) {
            return str == null || str.isEmpty();
        }
    }
}
