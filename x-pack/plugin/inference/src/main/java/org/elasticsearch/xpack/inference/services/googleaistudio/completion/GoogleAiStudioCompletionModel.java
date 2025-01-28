/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class GoogleAiStudioCompletionModel extends GoogleAiStudioModel {

    public GoogleAiStudioCompletionModel(
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
            GoogleAiStudioCompletionServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    GoogleAiStudioCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        GoogleAiStudioCompletionServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
    }

    public URI uri(boolean streaming) {
        try {
            var api = streaming ? GoogleAiStudioUtils.STREAM_GENERATE_CONTENT_ACTION : GoogleAiStudioUtils.GENERATE_CONTENT_ACTION;
            return new URIBuilder().setScheme("https")
                .setHost(GoogleAiStudioUtils.HOST_SUFFIX)
                .setPathSegments(GoogleAiStudioUtils.V1, GoogleAiStudioUtils.MODELS, format("%s:%s", getServiceSettings().modelId(), api))
                .build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GoogleAiStudioCompletionServiceSettings getServiceSettings() {
        return (GoogleAiStudioCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    // visible for testing
    static URI buildUri(String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GoogleAiStudioUtils.HOST_SUFFIX)
            .setPathSegments(
                GoogleAiStudioUtils.V1,
                GoogleAiStudioUtils.MODELS,
                format("%s:%s", model, GoogleAiStudioUtils.GENERATE_CONTENT_ACTION)
            )
            .build();
    }
}
