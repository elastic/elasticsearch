/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

public abstract class RateLimitGroupingModel extends Model {
    protected RateLimitGroupingModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected RateLimitGroupingModel(RateLimitGroupingModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    protected RateLimitGroupingModel(RateLimitGroupingModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public String inferenceEntityId() {
        return getInferenceEntityId();
    }

    public abstract int rateLimitGroupingHash();

    public abstract RateLimitSettings rateLimitSettings();
}
