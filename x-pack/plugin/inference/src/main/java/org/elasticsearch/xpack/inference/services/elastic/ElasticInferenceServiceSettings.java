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

    @Deprecated
    static final Setting<String> EIS_GATEWAY_URL = Setting.simpleString("xpack.inference.eis.gateway.url", Setting.Property.NodeScope);

    static final Setting<String> ELASTIC_INFERENCE_SERVICE_URL = Setting.simpleString(
        "xpack.inference.elastic.url",
        Setting.Property.NodeScope
    );

    // Adjust this variable to be volatile, if the setting can be updated at some point in time
    @Deprecated
    private final String eisGatewayUrl;

    private final String elasticInferenceServiceUrl;

    public ElasticInferenceServiceSettings(Settings settings) {
        eisGatewayUrl = EIS_GATEWAY_URL.get(settings);
        elasticInferenceServiceUrl = ELASTIC_INFERENCE_SERVICE_URL.get(settings);

    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(EIS_GATEWAY_URL, ELASTIC_INFERENCE_SERVICE_URL);
    }

    @Deprecated
    public String getEisGatewayUrl() {
        return eisGatewayUrl;
    }

    public String getElasticInferenceServiceUrl() {
        return elasticInferenceServiceUrl;
    }

}
