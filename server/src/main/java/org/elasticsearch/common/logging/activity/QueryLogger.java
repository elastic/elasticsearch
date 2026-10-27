/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.logging.Level;

import java.util.Optional;
import java.util.function.Predicate;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * The query log. It is an {@link ActivitySink} of the {@link ActivityInterceptor} of a query action, like any other sink, and
 * its switch, threshold, level and system index filter are driven by the {@code elasticsearch.querylog.*} settings.
 * Its producer and writer exchange {@link ESLogMessage}s.
 */
public class QueryLogger<Context extends QueryCaptureContext> {
    /** Prefix for query log related settings */
    public static final String QUERY_LOGGER_SETTINGS_PREFIX = "elasticsearch.querylog.";

    public static final Setting<Boolean> QUERY_LOGGER_ENABLED = boolSetting(
        QUERY_LOGGER_SETTINGS_PREFIX + "enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> QUERY_LOGGER_THRESHOLD = timeSetting(
        QUERY_LOGGER_SETTINGS_PREFIX + "threshold",
        TimeValue.MINUS_ONE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // Level for this log type. Presently set as operator configurable.
    public static final Setting<Level> QUERY_LOGGER_LEVEL = new Setting<>(
        QUERY_LOGGER_SETTINGS_PREFIX + "log_level",
        Level.INFO.name(),
        Level::valueOf,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    // Whether to include authentication information in the log
    public static final Setting<Boolean> QUERY_LOGGER_INCLUDE_USER = boolSetting(
        QUERY_LOGGER_SETTINGS_PREFIX + "include.user",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Whether to log searches that target system indices. */
    public static final Setting<Boolean> QUERY_LOGGER_LOG_SYSTEM = boolSetting(
        QUERY_LOGGER_SETTINGS_PREFIX + "include.system_indices",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ActivityLogProducer<Context> producer;
    private final ActivityLogWriter writer;
    private final ActionLoggingFields additionalFields;
    private final Predicate<String> systemChecker;
    private final ActivitySink<Context, ESLogMessage> sink;

    private volatile boolean enabled = false;
    private volatile long threshold = -1;
    private volatile Level logLevel = Level.INFO;
    private volatile boolean logSystemSearches = false;

    public QueryLogger(
        ClusterSettings settings,
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider
    ) {
        this(settings, producer, writerProvider, fieldsProvider, null);
    }

    @SuppressWarnings("this-escape")
    public QueryLogger(
        ClusterSettings settings,
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider,
        Predicate<String> systemChecker
    ) {
        this.producer = producer;
        this.writer = writerProvider.getWriter(producer.loggerName());
        var fieldsContext = new ActionLoggingFieldsContext(true);
        this.additionalFields = fieldsProvider.create(fieldsContext);
        this.systemChecker = systemChecker;
        this.sink = new ActivitySink<>(this::isEnabled, this::produce, this::write);
        settings.initializeAndWatch(QUERY_LOGGER_ENABLED, v -> enabled = v);
        settings.initializeAndWatch(QUERY_LOGGER_THRESHOLD, v -> threshold = v.nanos());
        settings.initializeAndWatch(QUERY_LOGGER_LEVEL, this::setLogLevel);
        settings.initializeAndWatch(QUERY_LOGGER_INCLUDE_USER, fieldsContext::setIncludeUserInformation);
        settings.initializeAndWatch(QUERY_LOGGER_LOG_SYSTEM, v -> logSystemSearches = v);
    }

    /**
     * The sink to register with the {@link ActivityInterceptor} of the action.
     */
    public ActivitySink<Context, ESLogMessage> sink() {
        return sink;
    }

    private void setLogLevel(Level level) {
        if (level.equals(Level.ERROR) || level.equals(Level.FATAL)) {
            throw new IllegalStateException("Log level can not be " + level.name());
        }
        logLevel = level;
    }

    private boolean isEnabled() {
        return enabled;
    }

    private Optional<ESLogMessage> produce(Context context) {
        boolean isSystemSearch = context.isSystemSearch(systemChecker);
        if (logSystemSearches == false && isSystemSearch) {
            return Optional.empty();
        }
        long threshold = this.threshold;
        if (threshold > 0 && context.getTookInNanos() < threshold) {
            return Optional.empty();
        }
        Optional<ESLogMessage> event = producer.produce(context, additionalFields);
        if (isSystemSearch) {
            event.ifPresent(message -> message.field(QueryLogging.QUERY_FIELD_IS_SYSTEM, true));
        }
        return event;
    }

    private void write(ESLogMessage message) {
        writer.write(logLevel, message);
    }
}
