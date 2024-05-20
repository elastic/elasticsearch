/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InferenceBaseRestTest extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .plugin("inference-service-test")
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

    protected Map<String, Object> putInferenceServiceModel(String modelId, TaskType taskType) throws IOException {
        String endpoint = org.elasticsearch.common.Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("PUT", endpoint);
        var modelConfig = ExampleModels.mockServiceModelConfig();
        request.setJsonEntity(modelConfig);
        var response = client().performRequest(request);
        return entityAsMap(response);
    }

    protected void putPyTorchModelTrainedModels(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId);
        var modelConfiguration = ExampleModels.pytorchPassThroughModelConfig();
        request.setJsonEntity(modelConfiguration);
        client().performRequest(request);
    }

    protected void putPyTorchModelVocabularyTrainedModels(List<String> vocabulary, String modelId) throws IOException {
        List<String> vocabularyWithPad = new ArrayList<>();
        vocabularyWithPad.add("[PAD]");
        vocabularyWithPad.add("[UNK]");
        vocabularyWithPad.addAll(vocabulary);
        String quotedWords = vocabularyWithPad.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));

        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/vocabulary");
        request.setJsonEntity(Strings.format("""
            { "vocabulary": [%s] }
            """, quotedWords));
        client().performRequest(request);
    }

    protected void putPyTorchModelDefinitionTrainedModels(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        String body = Strings.format(
            """
                {"total_definition_length":%s,"definition": "%s","total_parts": 1}""",
            ExampleModels.RAW_PYTORCH_MODEL_SIZE,
            ExampleModels.BASE_64_ENCODED_PYTORCH_MODEL
        );
        request.setJsonEntity(body);
        client().performRequest(request);
    }

    protected void startDeploymentTrainedModels(String modelId) throws IOException {
        String endPoint = "/_ml/trained_models/"
            + modelId
            + "/deployment/_start?timeout=40s&wait_for=started&threads_per_allocation=1&number_of_allocations=1";

        Request request = new Request("POST", endPoint);
        client().performRequest(request);
    }
}
