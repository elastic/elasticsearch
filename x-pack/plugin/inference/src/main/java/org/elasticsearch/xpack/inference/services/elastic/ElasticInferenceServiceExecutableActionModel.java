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
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.elastic.action.ElasticInferenceServiceActionVisitor;

import java.util.Map;

public abstract class ElasticInferenceServiceExecutableActionModel extends ElasticInferenceServiceModel {

    public ElasticInferenceServiceExecutableActionModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        ElasticInferenceServiceRateLimitServiceSettings rateLimitServiceSettings,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        super(configurations, secrets, rateLimitServiceSettings, elasticInferenceServiceComponents);
    }

    public ElasticInferenceServiceExecutableActionModel(
        ElasticInferenceServiceExecutableActionModel model,
        ServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    public abstract ExecutableAction accept(ElasticInferenceServiceActionVisitor visitor, Map<String, Object> taskSettings);
}
