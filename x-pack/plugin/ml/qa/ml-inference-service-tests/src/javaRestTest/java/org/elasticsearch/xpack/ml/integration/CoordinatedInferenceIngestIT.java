/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class CoordinatedInferenceIngestIT extends ESRestTestCase {

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

    @SuppressWarnings("unchecked")
    public void testIngestWithMultipleModelTypes() throws IOException {
        // Create an inference service model, dfa model and pytorch model
        var inferenceServiceModelId = "is_model";
        var boostedTreeModelId = "boosted_tree_model";
        var pyTorchModelId = "pytorch_model";

        putInferenceServiceModel(inferenceServiceModelId, TaskType.SPARSE_EMBEDDING);
        putBoostedTreeRegressionModel(boostedTreeModelId);
        putPyTorchModel(pyTorchModelId);
        putPyTorchModelDefinition(pyTorchModelId);
        putPyTorchModelVocabulary(List.of("these", "are", "my", "words"), pyTorchModelId);
        startDeployment(pyTorchModelId);

        String docs = """
            [
                {
                  "_source": {
                    "title": "my",
                    "body": "these are"
                  }
                },
                {
                  "_source": {
                    "title": "are",
                    "body": "my words"
                  }
                }
            ]
            """;

        {
            var responseMap = simulatePipeline(ExampleModels.nlpModelPipelineDefinition(inferenceServiceModelId), docs);
            var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
            System.out.println("IS DOCS " + simulatedDocs);
            assertThat(simulatedDocs, hasSize(2));
            assertEquals(inferenceServiceModelId, MapHelper.dig("doc._source.ml.model_id", simulatedDocs.get(0)));
            assertEquals("bar", MapHelper.dig("doc._source.ml.body", simulatedDocs.get(0)));
            assertEquals(inferenceServiceModelId, MapHelper.dig("doc._source.ml.model_id", simulatedDocs.get(1)));
            assertEquals("bar", MapHelper.dig("doc._source.ml.body", simulatedDocs.get(1)));
        }

        {
            var responseMap = simulatePipeline(ExampleModels.nlpModelPipelineDefinition(pyTorchModelId), docs);
            var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
            System.out.println("PT DOCS " + simulatedDocs);
            assertThat(simulatedDocs, hasSize(2));
            assertEquals(pyTorchModelId, MapHelper.dig("doc._source.ml.model_id", simulatedDocs.get(0)));
            List<List<Double>> results = (List<List<Double>>) MapHelper.dig("doc._source.ml.body", simulatedDocs.get(0));
            assertThat(results.get(0), contains(1.0, 1.0));
            assertEquals(pyTorchModelId, MapHelper.dig("doc._source.ml.model_id", simulatedDocs.get(1)));
        }

        String boostedTreeDocs = Strings.format("""
                [
                    {
                      "_source": %s
                    },
                    {
                      "_source": %s
                    }
                ]
                """, ExampleModels.randomBoostedTreeModelDoc(), ExampleModels.randomBoostedTreeModelDoc());
        {
            var responseMap = simulatePipeline(ExampleModels.boostedTreeRegressionModelPipelineDefinition(boostedTreeModelId), boostedTreeDocs);
            var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
            System.out.println("DFA DOCS " + simulatedDocs);
            assertThat(simulatedDocs, hasSize(2));
            assertEquals(boostedTreeModelId, MapHelper.dig("doc._source.ml.regression.model_id", simulatedDocs.get(0)));
            assertNotNull(MapHelper.dig("doc._source.ml.regression.predicted_value", simulatedDocs.get(0)));
            assertEquals(boostedTreeModelId, MapHelper.dig("doc._source.ml.regression.model_id", simulatedDocs.get(1)));
            assertNotNull(MapHelper.dig("doc._source.ml.regression.predicted_value", simulatedDocs.get(1)));
        }
    }

    @SuppressWarnings("unchecked")
    public void testPipelineConfiguredWithFieldMap() throws IOException {
        // Create an inference service model, dfa model and pytorch model
        var inferenceServiceModelId = "is_model";
        var boostedTreeModelId = "boosted_tree_model";
        var pyTorchModelId = "pytorch_model";

        putInferenceServiceModel(inferenceServiceModelId, TaskType.SPARSE_EMBEDDING);
        putBoostedTreeRegressionModel(boostedTreeModelId);
        putPyTorchModel(pyTorchModelId);
        putPyTorchModelDefinition(pyTorchModelId);
        putPyTorchModelVocabulary(List.of("these", "are", "my", "words"), pyTorchModelId);
        startDeployment(pyTorchModelId);

        String docs = """
            [
                {
                  "_source": {
                    "body": "these are"
                  }
                },
                {
                  "_source": {
                    "body": "my words"
                  }
                }
            ]
            """;

        {
            var responseMap = simulatePipeline(ExampleModels.nlpModelPipelineDefinitionWithFieldMap(pyTorchModelId), docs);
            var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
            System.out.println("DOCS pt" + simulatedDocs);
            assertThat(simulatedDocs, hasSize(2));
//            assertEquals(boostedTreeModelId, MapHelper.dig("doc._source.ml.regression.model_id", simulatedDocs.get(0)));
        }

        {
            var responseMap = simulatePipeline(ExampleModels.nlpModelPipelineDefinitionWithFieldMap(inferenceServiceModelId), docs);
            var simulatedDocs = (List<Map<String, Object>>) responseMap.get("docs");
            System.out.println("DOCS is" + simulatedDocs);
            assertThat(simulatedDocs, hasSize(2));
            // "[" + inferenceServiceModelId + "] is configured for the _inference API and does not accept documents as input"
//            assertEquals(boostedTreeModelId, MapHelper.dig("doc._source.ml.regression.model_id", simulatedDocs.get(0)));
        }



    }

    private Map<String, Object> putInferenceServiceModel(String modelId, TaskType taskType) throws IOException {
        String endpoint = org.elasticsearch.common.Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("PUT", endpoint);
        var modelConfig = ExampleModels.mockServiceModelConfig();
        request.setJsonEntity(modelConfig);
        var response = client().performRequest(request);
        return entityAsMap(response);
    }

    private void putPyTorchModel(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId);
        var modelConfiguration = ExampleModels.pytorchPassThroughModelConfig();
        request.setJsonEntity(modelConfiguration);
        client().performRequest(request);
    }

    protected void putPyTorchModelVocabulary(List<String> vocabulary, String modelId) throws IOException {
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

    protected Map<String, Object> simulatePipeline(String pipelineDef, String docs) throws IOException {
        String simulate = Strings.format("""
            {
              "pipeline": %s,
              "docs": %s
            }""", pipelineDef, docs);

        Request request = new Request("POST", "_ingest/pipeline/_simulate?error_trace=true");
        request.setJsonEntity(simulate);
        var response = client().performRequest(request);
        System.out.println(EntityUtils.toString(response.getEntity()));
        return entityAsMap(client().performRequest(request));
    }

    protected void putPyTorchModelDefinition(String modelId) throws IOException {
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

    protected void startDeployment(
        String modelId
    ) throws IOException {
        String endPoint = "/_ml/trained_models/"
            + modelId
            + "/deployment/_start?timeout=40s&wait_for=started&threads_per_allocation=1&number_of_allocations=1";

        Request request = new Request("POST", endPoint);
        client().performRequest(request);
    }

    private void putBoostedTreeRegressionModel(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId);
        var modelConfiguration = ExampleModels.boostedTreeRegressionModel();
        request.setJsonEntity(modelConfiguration);
        client().performRequest(request);
    }

    public Map<String, Object> getModel(String modelId, TaskType taskType) throws IOException {
        var endpoint = org.elasticsearch.common.Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("GET", endpoint);
        var reponse = client().performRequest(request);
        return entityAsMap(reponse);
    }
}
