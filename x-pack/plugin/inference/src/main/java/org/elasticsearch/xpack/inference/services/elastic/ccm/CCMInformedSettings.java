/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;

import java.util.Objects;

/**
 * Wraps the settings to default to a static URL if the user hasn't already set one using the
 * {@link ElasticInferenceServiceSettings#ELASTIC_INFERENCE_SERVICE_URL} setting.
 */
public class CCMInformedSettings extends ElasticInferenceServiceSettings {
    static final String DEFAULT_CCM_URL = "https://inference.us-east-1.aws.svc.elastic.cloud";

    private final CCMFeature ccmFeature;

    public CCMInformedSettings(Settings settings, CCMFeature ccmFeature) {
        super(settings);
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
    }

    @Override
    public String getElasticInferenceServiceUrl() {
        String urlFromSettings = super.getElasticInferenceServiceUrl();
        if (ccmFeature.isCcmSupportedEnvironment() == false || Strings.isNullOrEmpty(urlFromSettings) == false) {
            return urlFromSettings;
        }

        return DEFAULT_CCM_URL;
    }

}
