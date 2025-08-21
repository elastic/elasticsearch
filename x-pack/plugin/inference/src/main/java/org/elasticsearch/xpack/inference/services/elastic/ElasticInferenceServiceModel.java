/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

public abstract class ElasticInferenceServiceModel extends RateLimitGroupingModel {

    private final ElasticInferenceServiceRateLimitServiceSettings rateLimitServiceSettings;

    private final ElasticInferenceServiceComponents elasticInferenceServiceComponents;

    public ElasticInferenceServiceModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        ElasticInferenceServiceRateLimitServiceSettings rateLimitServiceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.elasticInferenceServiceComponents = Objects.requireNonNull(elasticInferenceServiceComponents);
    }

    public ElasticInferenceServiceModel(ElasticInferenceServiceModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        this.rateLimitServiceSettings = model.rateLimitServiceSettings;
        this.elasticInferenceServiceComponents = model.elasticInferenceServiceComponents();
    }

    @Override
    public int rateLimitGroupingHash() {
        // We only have one model for rerank
        return Objects.hash(this.getServiceSettings().modelId());
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitServiceSettings.rateLimitSettings();
    }

    public ElasticInferenceServiceComponents elasticInferenceServiceComponents() {
        return elasticInferenceServiceComponents;
    }
}
