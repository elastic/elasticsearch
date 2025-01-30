/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates settings using {@link Setting}. This does not represent service settings that are persisted
 * via {@link org.elasticsearch.inference.ServiceSettings}, but rather Elasticsearch settings passed on startup.
 */
public class ElasticInferenceServiceSettings {

    public static final String ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX = "xpack.inference.elastic.http.ssl.";

    @Deprecated
    static final Setting<String> EIS_GATEWAY_URL = Setting.simpleString("xpack.inference.eis.gateway.url", Setting.Property.NodeScope);

    static final Setting<String> ELASTIC_INFERENCE_SERVICE_URL = Setting.simpleString(
        "xpack.inference.elastic.url",
        Setting.Property.NodeScope
    );

    @Deprecated
    private final String eisGatewayUrl;

    private final String elasticInferenceServiceUrl;

    public ElasticInferenceServiceSettings(Settings settings) {
        eisGatewayUrl = EIS_GATEWAY_URL.get(settings);
        elasticInferenceServiceUrl = ELASTIC_INFERENCE_SERVICE_URL.get(settings);
    }

    public static final SSLConfigurationSettings ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_SETTINGS = SSLConfigurationSettings.withPrefix(
        ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX,
        false
    );

    public static final Setting<Boolean> ELASTIC_INFERENCE_SERVICE_SSL_ENABLED = Setting.boolSetting(
        ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "enabled",
        true,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.add(EIS_GATEWAY_URL);
        settings.add(ELASTIC_INFERENCE_SERVICE_URL);
        settings.add(ELASTIC_INFERENCE_SERVICE_SSL_ENABLED);
        settings.addAll(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_SETTINGS.getEnabledSettings());

        return settings;
    }

    public String getElasticInferenceServiceUrl() {
        return Strings.isEmpty(elasticInferenceServiceUrl) ? eisGatewayUrl : elasticInferenceServiceUrl;
    }

}
