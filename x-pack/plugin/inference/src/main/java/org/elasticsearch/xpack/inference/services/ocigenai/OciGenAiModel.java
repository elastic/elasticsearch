/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ocigenai.action.OciGenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiSecretsApplier;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * Base class of the OCI Generative AI models. Resolves the action URL from the service settings and holds the request signer that
 * adds the OCI request signature headers to outgoing requests.
 */
public abstract class OciGenAiModel extends RateLimitGroupingModel {

    private final URI uri;

    /**
     * Signs the outbound requests; holds the signing key parsed once from the stored secrets.
     */
    private final SecretsApplier secretsApplier;

    protected OciGenAiModel(ModelConfigurations configurations, ModelSecrets secrets, String action) {
        super(configurations, secrets);
        var serviceSettings = (OciGenAiServiceSettings) configurations.getServiceSettings();
        this.uri = OciGenAiUtils.buildUri(serviceSettings.uri(), serviceSettings.region(), serviceSettings.apiVersion(), action);
        this.secretsApplier = new OciGenAiSecretsApplier(
            configurations.getInferenceEntityId(),
            (OciGenAiSecretSettings) secrets.getSecretSettings()
        );
    }

    protected OciGenAiModel(OciGenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.uri = model.uri();
        this.secretsApplier = model.secretsApplier();
    }

    protected OciGenAiModel(OciGenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri();
        this.secretsApplier = model.secretsApplier();
    }

    public abstract ExecutableAction accept(OciGenAiActionVisitor creator, Map<String, Object> taskSettings);

    /**
     * @return the full URL of the OCI Generative AI action this model sends requests to
     */
    public URI uri() {
        return uri;
    }

    public SecretsApplier secretsApplier() {
        return secretsApplier;
    }

    @Override
    public OciGenAiServiceSettings getServiceSettings() {
        return (OciGenAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public OciGenAiSecretSettings getSecretSettings() {
        return (OciGenAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public int rateLimitGroupingHash() {
        // OCI Generative AI throughput limits apply per tenancy, region and model (or dedicated endpoint); the signing key does not
        // influence them.
        var serviceSettings = getServiceSettings();
        return Objects.hash(uri, serviceSettings.compartmentId(), serviceSettings.modelId(), serviceSettings.endpointId());
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }
}
