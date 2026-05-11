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
import java.util.Objects;

public abstract class CohereModel extends RateLimitGroupingModel {

    private final SecureString apiKey;
    private final RateLimitSettings rateLimitSettings;
    @Nullable
    private final URI testUri;

    public CohereModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        CohereCommonServiceSettings commonSettings
    ) {
        this(configurations, secrets, apiKeySecrets, commonSettings.rateLimitSettings(), commonSettings.uri());
    }

    protected CohereModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        @Nullable ApiKeySecrets apiKeySecrets,
        RateLimitSettings rateLimitSettings,
        @Nullable URI testUri
    ) {
        super(configurations, secrets);
        this.rateLimitSettings = Objects.requireNonNull(rateLimitSettings);
        this.apiKey = ServiceUtils.apiKey(apiKeySecrets);
        this.testUri = testUri;
    }

    protected CohereModel(CohereModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        rateLimitSettings = model.rateLimitSettings();
        apiKey = model.apiKey();
        testUri = model.testUri;
    }

    protected CohereModel(CohereModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        rateLimitSettings = model.rateLimitSettings();
        apiKey = model.apiKey();
        testUri = model.testUri;
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public RateLimitSettings rateLimitServiceSettings() {
        return rateLimitSettings;
    }

    public abstract ExecutableAction accept(CohereActionVisitor creator, Map<String, Object> taskSettings);

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
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
