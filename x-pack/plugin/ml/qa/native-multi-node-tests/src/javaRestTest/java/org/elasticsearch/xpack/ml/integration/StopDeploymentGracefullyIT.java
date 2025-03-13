/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;

import java.io.IOException;
import java.util.List;

public class StopDeploymentGracefullyIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testStopDeploymentGracefully() throws IOException {
        String baseModelId = "base-model";
        putAllModelParts(baseModelId);

        String forSearchDeploymentId = "for-search";
        startDeployment(baseModelId, forSearchDeploymentId, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        Response inference = infer("my words", forSearchDeploymentId);
        assertOK(inference);

        assertInferenceCountOnDeployment(1, forSearchDeploymentId);

        // infer by model Id
        inference = infer("my words", baseModelId);
        assertOK(inference);
        assertInferenceCountOnModel(2, baseModelId);

        stopDeployment(forSearchDeploymentId, false, true);
    }

    private void putAllModelParts(String modelId) throws IOException {
        createPassThroughModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
    }

    private void putModelDefinition(String modelId) throws IOException {
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
    }
}
