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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceCompletionModelTests extends ESTestCase {

    public void testOverridingModelId() {
        var originalModel = new ElasticInferenceServiceCompletionModel(
            "id",
            TaskType.COMPLETION,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings("model_id", new RateLimitSettings(100)),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of("url")
        );

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
}
