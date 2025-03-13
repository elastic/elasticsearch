/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.alibabacloudsearch.AlibabaCloudSearchActionVisitor;

import java.util.Map;
import java.util.Objects;

public abstract class AlibabaCloudSearchModel extends Model {
    private final AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings;

    public AlibabaCloudSearchModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);
        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    protected AlibabaCloudSearchModel(AlibabaCloudSearchModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    protected AlibabaCloudSearchModel(AlibabaCloudSearchModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(AlibabaCloudSearchActionVisitor creator, Map<String, Object> taskSettings, InputType inputType);

    public AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }
}
