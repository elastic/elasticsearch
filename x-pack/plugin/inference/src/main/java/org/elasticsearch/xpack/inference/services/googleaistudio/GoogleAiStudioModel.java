/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;

import java.util.Objects;

public abstract class GoogleAiStudioModel extends Model {

    private final GoogleAiStudioRateLimitServiceSettings rateLimitServiceSettings;

    public GoogleAiStudioModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        GoogleAiStudioRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    public GoogleAiStudioModel(GoogleAiStudioModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public GoogleAiStudioRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }
}
