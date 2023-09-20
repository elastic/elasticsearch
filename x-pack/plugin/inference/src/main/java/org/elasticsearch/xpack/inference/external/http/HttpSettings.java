/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;

public class HttpSettings {
    public static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting(
        "xpack.inference.http.max_response_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),   // default
        ByteSizeValue.ONE, // min
        new ByteSizeValue(50, ByteSizeUnit.MB),   // max
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> MAX_CONNECTIONS = Setting.intSetting(
        "xpack.inference.http.max_connections",
        500,
        1,
        // TODO pick a reasonable value here
        1000,
        Setting.Property.NodeScope
    );

    public static List<? extends Setting<?>> getSettings() {
        return List.of(MAX_HTTP_RESPONSE_SIZE, MAX_CONNECTIONS);
    }

    private HttpSettings() {}
}
