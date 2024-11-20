/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

public abstract class AmazonBedrockModel extends Model {

    protected String region;
    protected String model;
    protected AmazonBedrockProvider provider;
    protected RateLimitSettings rateLimitSettings;

    protected AmazonBedrockModel(ModelConfigurations modelConfigurations, ModelSecrets secrets) {
        super(modelConfigurations, secrets);
        setPropertiesFromServiceSettings((AmazonBedrockServiceSettings) modelConfigurations.getServiceSettings());
    }

    protected AmazonBedrockModel(Model model, TaskSettings taskSettings) {
        super(model, taskSettings);

        if (model instanceof AmazonBedrockModel bedrockModel) {
            setPropertiesFromServiceSettings(bedrockModel.getServiceSettings());
        }
    }

    protected AmazonBedrockModel(Model model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        if (serviceSettings instanceof AmazonBedrockServiceSettings bedrockServiceSettings) {
            setPropertiesFromServiceSettings(bedrockServiceSettings);
        }
    }

    protected AmazonBedrockModel(ModelConfigurations modelConfigurations) {
        super(modelConfigurations);
        setPropertiesFromServiceSettings((AmazonBedrockServiceSettings) modelConfigurations.getServiceSettings());
    }

    public String region() {
        return region;
    }

    public String model() {
        return model;
    }

    public AmazonBedrockProvider provider() {
        return provider;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    private void setPropertiesFromServiceSettings(AmazonBedrockServiceSettings serviceSettings) {
        this.region = serviceSettings.region();
        this.model = serviceSettings.modelId();
        this.provider = serviceSettings.provider();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
    }

    public abstract ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings);

    @Override
    public AmazonBedrockServiceSettings getServiceSettings() {
        return (AmazonBedrockServiceSettings) super.getServiceSettings();
    }

    @Override
    public AmazonBedrockSecretSettings getSecretSettings() {
        return (AmazonBedrockSecretSettings) super.getSecretSettings();
    }

}
