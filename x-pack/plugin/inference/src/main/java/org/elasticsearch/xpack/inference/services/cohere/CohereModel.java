/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.cohere.action.CohereActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;

public abstract class CohereModel<S extends CohereServiceSettings> extends RateLimitGroupingModel {

    @Nullable
    private final URI testUri;

    public CohereModel(ModelConfigurations configurations, ModelSecrets secrets, CohereCommonServiceSettings commonSettings) {
        this(configurations, secrets, commonSettings.uri());
    }

    protected CohereModel(ModelConfigurations configurations, ModelSecrets secrets, @Nullable URI testUri) {
        super(configurations, secrets);
        this.testUri = testUri;
    }

    protected CohereModel(CohereModel<?> model, TaskSettings taskSettings) {
        super(model, taskSettings);
        testUri = model.testUri;
    }

    protected CohereModel(CohereModel<?> model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        testUri = model.testUri;
    }

    @Override
    @SuppressWarnings("unchecked")
    public S getServiceSettings() {
        return (S) super.getServiceSettings();
    }

    public SecureString apiKey() {
        return ServiceUtils.apiKey((ApiKeySecrets) getSecretSettings());
    }

    public abstract ExecutableAction accept(CohereActionVisitor creator, Map<String, Object> taskSettings);

    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().commonSettings().rateLimitSettings();
    }

    public int rateLimitGroupingHash() {
        return apiKey().hashCode();
    }

    /** Returns a URI override for test use, or {@code null} to use the default Cohere endpoint. */
    @Nullable
    public URI testUri() {
        return testUri;
    }
}
