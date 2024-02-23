/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import static org.elasticsearch.xpack.inference.Utils.mockClusterService;

public class RequestExecutorServiceSettingsTests {
    public static RequestExecutorServiceSettings createRequestExecutorServiceSettingsEmpty() {
        return createRequestExecutorServiceSettings(Settings.EMPTY);
    }

    public static RequestExecutorServiceSettings createRequestExecutorServiceSettings(@Nullable Integer queueCapacity) {
        var settingsBuilder = Settings.builder();

        if (queueCapacity != null) {
            settingsBuilder.put(RequestExecutorServiceSettings.TASK_QUEUE_CAPACITY_SETTING.getKey(), queueCapacity);
        }

        return createRequestExecutorServiceSettings(settingsBuilder.build());
    }

    public static RequestExecutorServiceSettings createRequestExecutorServiceSettings(Settings settings) {
        return new RequestExecutorServiceSettings(settings, mockClusterService(settings));
    }
}
