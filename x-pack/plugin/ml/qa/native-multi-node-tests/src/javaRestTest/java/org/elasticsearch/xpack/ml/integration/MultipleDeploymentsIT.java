/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.List;

public class MultipleDeploymentsIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testDeployModelMultipleTimes() throws IOException {
        String baseModelId = "base-model";
        createPassThroughModel(baseModelId);
        putModelDefinition(baseModelId);
        putVocabulary(List.of("these", "are", "my", "words"), baseModelId);

        String forSearch = "for-search";
        startWithDeploymentId(baseModelId, forSearch);

        Response inference = infer("my words", forSearch);
        assertOK(inference);

        String forIngest = "for-ingest";
        startWithDeploymentId(baseModelId, forIngest);

        inference = infer("my words", forIngest);
        assertOK(inference);
        inference = infer("my words", forIngest);
        assertOK(inference);

        // TODO
        // assertInferenceCount(1, forSearch);
        // assertInferenceCount(2, forIngest);

        stopDeployment(forSearch);
        stopDeployment(forIngest);
    }

    private void putModelDefinition(String modelId) throws IOException {
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
    }
}
