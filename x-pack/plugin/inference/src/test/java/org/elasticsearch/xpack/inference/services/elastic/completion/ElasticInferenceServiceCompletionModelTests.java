/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceCompletionModelTests extends ESTestCase {

    public void testUriCreation() {
        var url = "http://eis-gateway.com";
        var model = createModel(url, "my-model-id");

        var uri = model.uri();
        assertThat(uri.toString(), is(url + "/api/v1/chat"));
    }

    public void testGetServiceSettings() {
        var modelId = "test-model";
        var model = createModel("http://eis-gateway.com", modelId);

        var serviceSettings = model.getServiceSettings();
        assertThat(serviceSettings.modelId(), is(modelId));
    }

    public void testGetTaskType() {
        var model = createModel("http://eis-gateway.com", "my-model-id");
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
        var originalModel = createModel("http://eis-gateway.com", "original-model");
        var newServiceSettings = new ElasticInferenceServiceCompletionServiceSettings("new-model");

        var overriddenModel = new ElasticInferenceServiceCompletionModel(originalModel, newServiceSettings);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("new-model"));
        assertThat(overriddenModel.getTaskType(), is(TaskType.COMPLETION));
        assertThat(overriddenModel.uri().toString(), is(originalModel.uri().toString()));
    }

    public void testUriCreationWithInvalidUrl() {
        var invalidUrl = "not-a-valid-url";
        var modelId = "my-model-id";

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> new ElasticInferenceServiceCompletionModel(
                "id",
                TaskType.COMPLETION,
                "elastic",
                new ElasticInferenceServiceCompletionServiceSettings(modelId),
                EmptyTaskSettings.INSTANCE,
                EmptySecretSettings.INSTANCE,
                ElasticInferenceServiceComponents.of(invalidUrl)
            )
        );

        assertThat(exception.status().getStatus(), is(400));
        assertThat(exception.getMessage(), Matchers.containsString("Failed to create URI"));
    }

    public static ElasticInferenceServiceCompletionModel createModel(String url, String modelId) {
        return new ElasticInferenceServiceCompletionModel(
            "id",
            TaskType.COMPLETION,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings(modelId),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );
    }
}
