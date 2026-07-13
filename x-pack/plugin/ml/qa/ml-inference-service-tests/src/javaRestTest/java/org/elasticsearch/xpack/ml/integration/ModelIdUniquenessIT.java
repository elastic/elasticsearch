/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.inference.TaskType;
import org.hamcrest.Matchers;

import java.util.List;

public class ModelIdUniquenessIT extends InferenceBaseRestTest {

    public void testPutInferenceModelFailsWhenTrainedModelWithIdAlreadyExists() throws Exception {

        String modelId = "duplicate_model_id";
        putPyTorchModelTrainedModels(modelId);
        putPyTorchModelDefinitionTrainedModels(modelId);
        putPyTorchModelVocabularyTrainedModels(List.of("these", "are", "my", "words"), modelId);
        startDeploymentTrainedModels(modelId);

        var e = expectThrows(ResponseException.class, () -> putInferenceServiceModel(modelId, TaskType.SPARSE_EMBEDDING));
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                "Inference endpoint IDs must be unique. Requested inference endpoint ID ["
                    + modelId
                    + "] matches existing trained model ID(s) but must not."
            )
        );
    }

    public void testPutTrainedModelFailsWhenInferenceModelWithIdAlreadyExists() throws Exception {

        String modelId = "duplicate_model_id";
        putPyTorchModelTrainedModels(modelId);
        putPyTorchModelDefinitionTrainedModels(modelId);
        putPyTorchModelVocabularyTrainedModels(List.of("these", "are", "my", "words"), modelId);

        putInferenceServiceModel(modelId, TaskType.SPARSE_EMBEDDING);

        var e = expectThrows(ResponseException.class, () -> startDeploymentTrainedModels(modelId));
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                "Model IDs must be unique. Requested model ID [" + modelId + "] matches existing model IDs but must not."
            )

        );

    }
}
