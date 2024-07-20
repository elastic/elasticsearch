/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Objects;

public class HttpSettings {
    // These settings are default scope for testing
    static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting(
        "xpack.inference.http.max_response_size",
        new ByteSizeValue(50, ByteSizeUnit.MB),   // default
        ByteSizeValue.ONE, // min
        new ByteSizeValue(100, ByteSizeUnit.MB),   // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile ByteSizeValue maxResponseSize;

    public HttpSettings(Settings settings, ClusterService clusterService) {
        Objects.requireNonNull(clusterService);
        Objects.requireNonNull(settings);
        maxResponseSize = MAX_HTTP_RESPONSE_SIZE.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_HTTP_RESPONSE_SIZE, this::setMaxResponseSize);
    }

    public ByteSizeValue getMaxResponseSize() {
        return maxResponseSize;
    }

    private void setMaxResponseSize(ByteSizeValue maxResponseSize) {
        this.maxResponseSize = maxResponseSize;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(MAX_HTTP_RESPONSE_SIZE);
    }
}
