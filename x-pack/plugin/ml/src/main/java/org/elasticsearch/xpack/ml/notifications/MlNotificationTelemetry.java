/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.util.Map;
import java.util.Objects;

/**
 * Per-node count of ML notifications by level. One instance is shared by all auditors because an
 * instrument name can be registered only once. The message and resource id are never recorded.
 */
public final class MlNotificationTelemetry {

    public static final String NOTIFICATIONS_METRIC = "es.ml.notifications.total";
    public static final String LEVEL_ATTRIBUTE = "es_notification_level";

    private final LongCounter notificationCounter;

    public MlNotificationTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.notificationCounter = meterRegistry.registerLongCounter(
            NOTIFICATIONS_METRIC,
            "Count of ML notifications issued on this node, by level.",
            "notifications"
        );
    }

    public void recordNotification(Level level) {
        notificationCounter.incrementBy(1, Map.of(LEVEL_ATTRIBUTE, level.toString()));
    }
}
