/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.googleaistudio.GoogleAiStudioActionVisitor;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioUtils;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class GoogleAiStudioEmbeddingsModel extends GoogleAiStudioModel {

    private URI uri;

    public GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            GoogleAiStudioEmbeddingsServiceSettings.fromMap(serviceSettings),
            EmptyTaskSettings.INSTANCE,
            GoogleAiStudioSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable GoogleAiStudioSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    GoogleAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        GoogleAiStudioEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable GoogleAiStudioSecretSettings secrets
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
    public GoogleAiStudioEmbeddingsServiceSettings getServiceSettings() {
        return (GoogleAiStudioEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public GoogleAiStudioSecretSettings getSecretSettings() {
        return (GoogleAiStudioSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return uri;
    }

    @Override
    public ExecutableAction accept(GoogleAiStudioActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleAiStudioUtils.HOST_SUFFIX)
            .setPathSegments(
                GoogleAiStudioUtils.V1,
                GoogleAiStudioUtils.MODELS,
                format("%s:%s", model, GoogleAiStudioUtils.BATCH_EMBED_CONTENTS_ACTION)
            )
            .build();
    }
}
