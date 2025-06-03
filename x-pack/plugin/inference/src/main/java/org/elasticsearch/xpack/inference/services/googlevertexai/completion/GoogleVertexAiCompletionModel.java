/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionVisitor;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiCompletionModel extends GoogleVertexAiChatCompletionModel {

    public GoogleVertexAiCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, taskSettings, secrets, context);
        try {
            var modelServiceSettings = this.getServiceSettings();
            this.uri = buildUri(modelServiceSettings.location(), modelServiceSettings.projectId(), modelServiceSettings.modelId());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }

    public void updateUri(boolean isStream) throws URISyntaxException {
        var location = getServiceSettings().location();
        var projectId = getServiceSettings().projectId();
        var model = getServiceSettings().modelId();

        // Google VertexAI generates streaming response using another API. We call this
        // method before making the request to be sure we are calling the right API
        if (isStream) {
            this.uri = GoogleVertexAiChatCompletionModel.buildUri(location, projectId, model);
        } else {
            this.uri = GoogleVertexAiCompletionModel.buildUri(location, projectId, model);
        }
    }

    @Override
    public ExecutableAction accept(GoogleVertexAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String location, String projectId, String model) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(format("%s%s", location, GoogleVertexAiUtils.GOOGLE_VERTEX_AI_HOST_SUFFIX))
            .setPathSegments(
                GoogleVertexAiUtils.V1,
                GoogleVertexAiUtils.PROJECTS,
                projectId,
                GoogleVertexAiUtils.LOCATIONS,
                GoogleVertexAiUtils.GLOBAL,
                GoogleVertexAiUtils.PUBLISHERS,
                GoogleVertexAiUtils.PUBLISHER_GOOGLE,
                GoogleVertexAiUtils.MODELS,
                format("%s:%s", model, GoogleVertexAiUtils.GENERATE_CONTENT)
            )
            .build();
    }
}
