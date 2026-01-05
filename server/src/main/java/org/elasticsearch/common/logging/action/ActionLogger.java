/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * Generic wrapper to log completion (whether successful or not) of any action, with necessary details.
 * Specific details are added in the specific context types for each action.
 * @param <Context> Logging context type
 */
public class ActionLogger<Context extends ActionLoggerContext> {
    private final ActionLoggerProducer<Context> producer;
    private final ActionLogWriter writer;
    private final SlowLogFields additionalFields;
    private boolean enabled = false;
    private long threshold = -1;

    public static final String ACTION_LOGGER_SETTINGS_PREFIX = "elasticsearch.actionlog.";
    public static final Setting.AffixSetting<Boolean> SEARCH_ACTION_LOGGER_ENABLED = Setting.affixKeySetting(
        ACTION_LOGGER_SETTINGS_PREFIX,
        "enabled",
        key -> boolSetting(key, false, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> SEARCH_ACTION_LOGGER_THRESHOLD = Setting.affixKeySetting(
        ACTION_LOGGER_SETTINGS_PREFIX,
        "threshold",
        key -> timeSetting(key, TimeValue.MINUS_ONE, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public ActionLogger(
        String name,
        ClusterSettings settings,
        ActionLoggerProducer<Context> producer,
        ActionLogWriter writer,
        SlowLogFieldProvider slowLogFieldProvider
    ) {
        this.producer = producer;
        this.writer = writer;
        this.additionalFields = slowLogFieldProvider.create();
        settings.addAffixUpdateConsumer(SEARCH_ACTION_LOGGER_ENABLED, (k, v) -> { if (name.equals(k)) enabled = v; }, (k, v) -> {});
        settings.addAffixUpdateConsumer(
            SEARCH_ACTION_LOGGER_THRESHOLD,
            (k, v) -> { if (name.equals(k)) threshold = v.nanos(); },
            (k, v) -> {}
        );
    }

    // Accessible for tests
    void logAction(Context context) {
        if (enabled == false || (threshold > -1 && context.getTookInNanos() < threshold)) {
            return;
        }
        Level level = producer.logLevel(context);
        if (level.equals(Level.OFF)) {
            return;
        }
        var event = producer.produce(context, additionalFields);
        writer.write(level, event);
    }

    public <Req, R> ActionListener<R> wrap(ActionListener<R> listener, final ActionLoggerContextBuilder<Context, Req, R> contextBuilder) {
        if (enabled == false) {
            return listener;
        }
        return new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(R r) {
                var ctx = contextBuilder.build(r);
                logAction(ctx);
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                var ctx = contextBuilder.build(e);
                logAction(ctx);
                super.onFailure(e);
            }

            @Override
            public String toString() {
                return "ActionLogger listener/" + delegate;
            }
        };
    }
}
