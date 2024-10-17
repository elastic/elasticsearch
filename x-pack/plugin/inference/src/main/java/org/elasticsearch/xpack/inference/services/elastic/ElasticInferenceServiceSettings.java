/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class ElasticInferenceServiceSettings {

    static final Setting<String> EIS_GATEWAY_URL = Setting.simpleString("xpack.inference.eis.gateway.url", Setting.Property.NodeScope);

    // Adjust this variable to be volatile, if the setting can be updated at some point in time
    private final String eisGatewayUrl;

    public ElasticInferenceServiceSettings(Settings settings) {
        eisGatewayUrl = EIS_GATEWAY_URL.get(settings);
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(EIS_GATEWAY_URL);
    }

    public String getEisGatewayUrl() {
        return eisGatewayUrl;
    }
}
