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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.util.List;

public class ModelIdUniquenessIT extends InferenceBaseRestTest {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .plugin("org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

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
                "Model IDs must be unique. Requested model ID [" + modelId + "] matches existing model IDs but must not."
            )

        );
    }

    public void testPutTrainedModelFailsWhenInferenceModelWithIdAlreadyExists() throws Exception {

        String modelId = "duplicate_model_id";
        putPyTorchModelTrainedModels(modelId);
        putPyTorchModelDefinitionTrainedModels(modelId);
        putPyTorchModelVocabularyTrainedModels(List.of("these", "are", "my", "words"), modelId);
        startDeploymentTrainedModels(modelId);

        var e = expectThrows(ResponseException.class, () -> putInferenceServiceModel(modelId, TaskType.SPARSE_EMBEDDING));
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                "Model IDs must be unique. Requested model ID [" + modelId + "] matches existing model IDs but must not."
            )

        );

    }
}
