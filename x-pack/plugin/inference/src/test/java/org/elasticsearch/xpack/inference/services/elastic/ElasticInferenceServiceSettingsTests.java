/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ElasticInferenceServiceSettingsTests extends ESTestCase {

    private static final String ELASTIC_INFERENCE_SERVICE_URL = "http://elastic-inference-service";
    private static final String ELASTIC_INFERENCE_SERVICE_LEGACY_URL = "http://elastic-inference-service-legacy";

    public static ElasticInferenceServiceSettings create(String elasticInferenceServiceUrl) {
        var settings = Settings.builder()
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), elasticInferenceServiceUrl)
            .build();

        return new ElasticInferenceServiceSettings(settings);
    }

    public static ElasticInferenceServiceSettings create(
        String elasticInferenceServiceUrl,
        TimeValue authorizationRequestInterval,
        TimeValue maxJitter,
        boolean periodicAuthorizationEnabled
    ) {
        var settings = Settings.builder()
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), elasticInferenceServiceUrl)
            .put(ElasticInferenceServiceSettings.AUTHORIZATION_REQUEST_INTERVAL.getKey(), authorizationRequestInterval)
            .put(ElasticInferenceServiceSettings.MAX_AUTHORIZATION_REQUEST_JITTER.getKey(), maxJitter)
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), periodicAuthorizationEnabled)
            .build();

        return new ElasticInferenceServiceSettings(settings);
    }

    public void testGetElasticInferenceServiceUrl_WithUrlSetting() {
        var settings = Settings.builder()
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), ELASTIC_INFERENCE_SERVICE_URL)
            .build();

        var eisSettings = new ElasticInferenceServiceSettings(settings);

        assertThat(eisSettings.getElasticInferenceServiceUrl(), equalTo(ELASTIC_INFERENCE_SERVICE_URL));
    }

    public void testGetElasticInferenceServiceUrl_WithLegacyUrlSetting() {
        var settings = Settings.builder()
            .put(ElasticInferenceServiceSettings.EIS_GATEWAY_URL.getKey(), ELASTIC_INFERENCE_SERVICE_LEGACY_URL)
            .build();

        var eisSettings = new ElasticInferenceServiceSettings(settings);

        assertThat(eisSettings.getElasticInferenceServiceUrl(), equalTo(ELASTIC_INFERENCE_SERVICE_LEGACY_URL));
    }

    public void testGetElasticInferenceServiceUrl_WithUrlSetting_TakesPrecedenceOverLegacyUrlSetting() {
        var settings = Settings.builder()
            .put(ElasticInferenceServiceSettings.EIS_GATEWAY_URL.getKey(), ELASTIC_INFERENCE_SERVICE_LEGACY_URL)
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), ELASTIC_INFERENCE_SERVICE_URL)
            .build();

        var eisSettings = new ElasticInferenceServiceSettings(settings);

        assertThat(eisSettings.getElasticInferenceServiceUrl(), equalTo(ELASTIC_INFERENCE_SERVICE_URL));
    }

    public void testGetElasticInferenceServiceUrl_WithoutUrlSetting() {
        var eisSettings = new ElasticInferenceServiceSettings(Settings.EMPTY);

        assertThat(eisSettings.getElasticInferenceServiceUrl(), equalTo(""));
    }
}
