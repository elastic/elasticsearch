/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.xpack.inference.Utils.mockClusterService;

public class RequestBatchingServiceSettingsTests {
    public static RequestBatchingServiceSettings createRequestBatchingServiceSettingsEmpty() {
        return createRequestBatchingServiceSettings(Settings.EMPTY);
    }

    public static RequestBatchingServiceSettings createRequestBatchingServiceSettings(
        @Nullable TimeValue waitPeriod,
        @Nullable Integer executionThreshold,
        @Nullable Integer queueCapacity
    ) {
        var settingsBuilder = Settings.builder();

        if (waitPeriod != null) {
            settingsBuilder.put(RequestBatchingServiceSettings.BATCHING_WAIT_PERIOD_SETTING.getKey(), waitPeriod);
        }

        if (executionThreshold != null) {
            settingsBuilder.put(RequestBatchingServiceSettings.BATCH_EXECUTION_THRESHOLD_SETTING.getKey(), executionThreshold);
        }

        if (queueCapacity != null) {
            settingsBuilder.put(RequestBatchingServiceSettings.TASK_QUEUE_CAPACITY_SETTING.getKey(), queueCapacity);
        }

        return createRequestBatchingServiceSettings(settingsBuilder.build());
    }

    public static RequestBatchingServiceSettings createRequestBatchingServiceSettings(Settings settings) {
        return new RequestBatchingServiceSettings(settings, mockClusterService(settings));
    }
}
