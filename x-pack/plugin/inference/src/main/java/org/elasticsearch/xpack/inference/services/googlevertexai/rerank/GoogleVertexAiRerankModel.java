/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiRerankModel extends GoogleVertexAiModel {

    public GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleVertexAiRerankServiceSettings.fromMap(serviceSettings, context),
            GoogleVertexAiRerankTaskSettings.fromMap(taskSettings),
            GoogleVertexAiSecretSettings.fromMap(secrets)
        );
    }

    public GoogleVertexAiRerankModel(GoogleVertexAiRerankModel model, GoogleVertexAiRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    // Should only be used directly for testing
    GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleVertexAiRerankServiceSettings serviceSettings,
        GoogleVertexAiRerankTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.projectId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    protected GoogleVertexAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleVertexAiRerankServiceSettings serviceSettings,
        GoogleVertexAiRerankTaskSettings taskSettings,
        @Nullable GoogleVertexAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GoogleVertexAiRerankServiceSettings getServiceSettings() {
        return (GoogleVertexAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public GoogleVertexAiRerankTaskSettings getTaskSettings() {
        return (GoogleVertexAiRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public GoogleVertexAiSecretSettings getSecretSettings() {
        return (GoogleVertexAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public GoogleDiscoveryEngineRateLimitServiceSettings rateLimitServiceSettings() {
        return (GoogleDiscoveryEngineRateLimitServiceSettings) super.rateLimitServiceSettings();
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String projectId) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleVertexAiUtils.GOOGLE_DISCOVERY_ENGINE_HOST)
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                GoogleVertexAiUtils.GLOBAL,
                GoogleVertexAiUtils.RANKING_CONFIGS,
                format("%s:%s", GoogleVertexAiUtils.DEFAULT_RANKING_CONFIG, GoogleVertexAiUtils.RANK)
            )
            .build();
    }
}
