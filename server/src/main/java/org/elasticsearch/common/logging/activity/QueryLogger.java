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
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.logging.Level;

import java.util.function.Predicate;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class QueryLogger<Context extends QueryLoggerContext> extends ActivityLogger<Context> {
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

    private boolean logSystemSearches = false;
    private final Predicate<String> systemChecker;

    @Override
    protected boolean shouldLog(Context context) {
        if (logSystemSearches == false && context.isSystemSearch(systemChecker)) {
            return false;
        }
        return super.shouldLog(context);
    }

    public QueryLogger(
        ClusterSettings settings,
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider
    ) {
        this(settings, producer, writerProvider, fieldsProvider, null);
    }

    public QueryLogger(
        ClusterSettings settings,
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider,
        Predicate<String> systemChecker
    ) {
        super(producer, writerProvider, fieldsProvider);
        settings.initializeAndWatch(QUERY_LOGGER_ENABLED, v -> enabled = v);
        settings.initializeAndWatch(QUERY_LOGGER_THRESHOLD, v -> threshold = v.nanos());
        settings.initializeAndWatch(QUERY_LOGGER_LEVEL, this::setLogLevel);
        settings.initializeAndWatch(QUERY_LOGGER_INCLUDE_USER, this.setincludeUserInformation);
        settings.initializeAndWatch(QUERY_LOGGER_LOG_SYSTEM, this::setLogSystemSearches);
        this.systemChecker = systemChecker;
    }

    public void setLogSystemSearches(boolean logSystemSearches) {
        this.logSystemSearches = logSystemSearches;
    }

    protected void addFields(Context context, ESLogMessage logMessage) {
        if (context.isSystemSearch(systemChecker)) {
            logMessage.field(QueryLogging.QUERY_FIELD_IS_SYSTEM, true);
        }
    }

}
