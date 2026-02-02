/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceCompletionModelTests extends ESTestCase {

    public void testOverridingModelId() {
        var originalModel = createModel("url", "model_id", TaskType.COMPLETION);

        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("message"), "user", null, null)),
            "new_model_id",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = ElasticInferenceServiceCompletionModel.of(originalModel, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("new_model_id"));
        assertThat(overriddenModel.getTaskType(), is(TaskType.COMPLETION));
    }

    public void testUriCreation() {
        var model = createModel("http://eis-gateway.com", "my-model-id", TaskType.COMPLETION);

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/chat"));
    }

    public void testUriCreation_WithTrailingSlash() {
        var model = createModel("http://eis-gateway.com/", "my-model-id", TaskType.COMPLETION);

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/chat"));
    }

    public void testGetServiceSettings() {
        var modelId = "test-model";
        var model = createModel("http://eis-gateway.com", modelId, TaskType.COMPLETION);

        var serviceSettings = model.getServiceSettings();
        assertThat(serviceSettings.modelId(), is(modelId));
    }

    public void testGetTaskType() {
        var model = createModel("http://eis-gateway.com", "my-model-id", TaskType.COMPLETION);
        assertThat(model.getTaskType(), is(TaskType.COMPLETION));
    }

    public void testGetInferenceEntityId() {
        var inferenceEntityId = "test-id";
        var model = new ElasticInferenceServiceCompletionModel(
            inferenceEntityId,
            TaskType.COMPLETION,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings("my-model-id"),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of("http://eis-gateway.com")
        );

        assertThat(model.getInferenceEntityId(), is(inferenceEntityId));
    }

    public void testModelWithOverriddenServiceSettings() {
        var originalModel = createModel("http://eis-gateway.com", "original-model", TaskType.COMPLETION);
        var newServiceSettings = new ElasticInferenceServiceCompletionServiceSettings("new-model");

        var overriddenModel = new ElasticInferenceServiceCompletionModel(originalModel, newServiceSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("new-model"));
        assertThat(overriddenModel.getTaskType(), is(TaskType.COMPLETION));
        assertThat(overriddenModel.uri().toString(), is(originalModel.uri().toString()));
    }

    public static ElasticInferenceServiceCompletionModel createModel(String url, String modelId, TaskType taskType) {
        return new ElasticInferenceServiceCompletionModel(
            "id",
            taskType,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings(modelId),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );
    }
}
