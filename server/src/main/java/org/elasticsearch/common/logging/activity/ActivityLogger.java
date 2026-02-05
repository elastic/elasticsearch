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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.logging.Level;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

    public static final String ACTIVITY_LOGGER_SETTINGS_PREFIX = "elasticsearch.actionlog.";
    public static final Setting.AffixSetting<Boolean> ACTIVITY_LOGGER_ENABLED = Setting.affixKeySetting(
        ACTIVITY_LOGGER_SETTINGS_PREFIX,
        "enabled",
        key -> boolSetting(key, false, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> ACTIVITY_LOGGER_THRESHOLD = Setting.affixKeySetting(
        ACTIVITY_LOGGER_SETTINGS_PREFIX,
        "threshold",
        key -> timeSetting(key, TimeValue.MINUS_ONE, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    // Default log level for this log type. Logger can override that if it wants to.
    public static final Setting.AffixSetting<Level> ACTIVITY_LOGGER_LEVEL = Setting.affixKeySetting(
        ACTIVITY_LOGGER_SETTINGS_PREFIX,
        "log_level",
        key -> new Setting<>(key, Level.INFO.name(), Level::valueOf, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    // Whether to include authentication information in the log
    public static final Setting.AffixSetting<Boolean> ACTIVITY_LOGGER_INCLUDE_USER = Setting.affixKeySetting(
        ACTIVITY_LOGGER_SETTINGS_PREFIX,
        // Named to match slowlog, we may reconsider this naming
        "include.user",
        key -> boolSetting(key, true, Setting.Property.Dynamic, Setting.Property.NodeScope)
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
        // Initialize
        this.additionalFields = fieldsProvider.create(context);
        this.enabled = settings.get(ACTIVITY_LOGGER_ENABLED.getConcreteSettingForNamespace(name));
        this.threshold = settings.get(ACTIVITY_LOGGER_THRESHOLD.getConcreteSettingForNamespace(name)).nanos();
        setLogLevel(settings.get(ACTIVITY_LOGGER_LEVEL.getConcreteSettingForNamespace(name)));
        context.setIncludeUserInformation(settings.get(ACTIVITY_LOGGER_INCLUDE_USER.getConcreteSettingForNamespace(name)));

        settings.addAffixUpdateConsumer(ACTIVITY_LOGGER_ENABLED, updater(name, v -> enabled = v), (k, v) -> {});
        settings.addAffixUpdateConsumer(ACTIVITY_LOGGER_THRESHOLD, updater(name, v -> threshold = v.nanos()), (k, v) -> {});
        settings.addAffixUpdateConsumer(ACTIVITY_LOGGER_LEVEL, updater(name, this::setLogLevel), (k, v) -> {
            if (v.equals(Level.ERROR) || v.equals(Level.FATAL)) {
                throw new IllegalStateException("Log level can not be " + v.name() + " for " + k);
            }
        });
        settings.addAffixUpdateConsumer(ACTIVITY_LOGGER_INCLUDE_USER, updater(name, context::setIncludeUserInformation), (k, v) -> {});
    }

    private void setLogLevel(Level level) {
        if (level.equals(Level.ERROR) || level.equals(Level.FATAL)) {
            throw new IllegalStateException("Log level can not be " + level.name());
        }
        logLevel = level;
    }

    private <T> BiConsumer<String, T> updater(String name, Consumer<T> updater) {
        return (k, v) -> { if (name.equals(k)) updater.accept(v); };
    }

    // Accessible for tests
    void logAction(Context context) {
        if (enabled == false || (threshold > -1 && context.getTookInNanos() < threshold)) {
            return;
        }
        Level level = producer.logLevel(context, logLevel);
        if (level.equals(Level.OFF)) {
            return;
        }
        var event = producer.produce(context, additionalFields);
        if (event != null) {
            writer.write(level, event);
        }
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
