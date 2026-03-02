/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.logging.Level;

import java.util.Optional;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * Generic wrapper to log completion (whether successful or not) of any action, with necessary details.
 * Specific details are added in the specific context types for each action, such as search, ESQL query, etc.
 * @param <Context> Logging context type
 */
public class ActivityLogger<Context extends ActivityLoggerContext> {
    private final ActivityLogProducer<Context> producer;
    private final ActivityLogWriter writer;
    private final ActionLoggingFields additionalFields;
    private boolean enabled = false;
    private long threshold = -1;
    private Level logLevel = Level.INFO;

    /** Prefix for activity log related settings; used by subclasses for their own settings (e.g. search). */
    public static final String ACTIVITY_LOGGER_SETTINGS_PREFIX = "elasticsearch.activitylog.";

    public static final Setting<Boolean> ACTIVITY_LOGGER_ENABLED = boolSetting(
        "elasticsearch.activitylog.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> ACTIVITY_LOGGER_THRESHOLD = timeSetting(
        "elasticsearch.activitylog.threshold",
        TimeValue.MINUS_ONE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // Level for this log type. Presently set as operator configurable.
    public static final Setting<Level> ACTIVITY_LOGGER_LEVEL = new Setting<>(
        "elasticsearch.activitylog.log_level",
        Level.INFO.name(),
        Level::valueOf,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    // Whether to include authentication information in the log
    public static final Setting<Boolean> ACTIVITY_LOGGER_INCLUDE_USER = boolSetting(
        "elasticsearch.activitylog.include.user",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public ActivityLogger(
        String name,
        ClusterSettings settings,
        ActivityLogProducer<Context> producer,
        ActivityLogWriterProvider writerProvider,
        ActionLoggingFieldsProvider fieldsProvider
    ) {
        this.producer = producer;
        this.writer = writerProvider.getWriter(producer.loggerName());
        var context = new ActionLoggingFieldsContext(true);
        this.additionalFields = fieldsProvider.create(context);

        settings.initializeAndWatch(ACTIVITY_LOGGER_ENABLED, v -> enabled = v);
        settings.initializeAndWatch(ACTIVITY_LOGGER_THRESHOLD, v -> threshold = v.nanos());
        settings.initializeAndWatch(ACTIVITY_LOGGER_LEVEL, this::setLogLevel);
        settings.initializeAndWatch(ACTIVITY_LOGGER_INCLUDE_USER, context::setIncludeUserInformation);
    }

    private void setLogLevel(Level level) {
        if (level.equals(Level.ERROR) || level.equals(Level.FATAL)) {
            throw new IllegalStateException("Log level can not be " + level.name());
        }
        logLevel = level;
    }

    // For tests
    public Level getLogLevel() {
        return logLevel;
    }

    // Accessible for tests
    void logAction(Context context) {
        if (enabled == false || (threshold > -1 && context.getTookInNanos() < threshold)) {
            return;
        }
        Optional<ESLogMessage> event = producer.produce(context, additionalFields);
        event.ifPresent(logMessage -> writer.write(logLevel, logMessage));
    }

    public <Req, R> ActionListener<R> wrap(ActionListener<R> listener, final ActivityLoggerContextBuilder<Context, Req, R> contextBuilder) {
        if (enabled == false) {
            return listener;
        }
        return new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(R r) {
                log(r);
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                log(e);
                super.onFailure(e);
            }

            private void log(R r) {
                Context ctx = contextBuilder.build(r);
                logAction(ctx);
            }

            private void log(Exception e) {
                Context ctx = contextBuilder.build(e);
                logAction(ctx);
            }

            @Override
            public String toString() {
                return "ActivityLogger listener/" + delegate;
            }
        };
    }
}
