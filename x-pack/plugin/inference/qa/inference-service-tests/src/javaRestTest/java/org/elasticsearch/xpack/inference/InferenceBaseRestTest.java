/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InferenceBaseRestTest extends ESRestTestCase {
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

    static String mockServiceModelConfig() {
        return """
            {
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """;
    }

    // basic model from xpack/ml/integration/PyTorchModelIT.java
    static final String BASE_64_ENCODED_MODEL =
        "UEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAUAA4Ac2ltcGxlbW9kZWwvZGF0YS5wa2xGQgoAWlpaWlpaWlpaWoACY19fdG9yY2hfXwp"
            + "TdXBlclNpbXBsZQpxACmBfShYCAAAAHRyYWluaW5ncQGIdWJxAi5QSwcIXOpBBDQAAAA0AAAAUEsDBBQACAgIAAAAAAAAAAAAAAAAAA"
            + "AAAAAdAEEAc2ltcGxlbW9kZWwvY29kZS9fX3RvcmNoX18ucHlGQj0AWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaW"
            + "lpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWnWOMWvDMBCF9/yKI5MMrnHTQsHgjt2aJdlCEIp9SgWSTpykFvfXV1htaYds0nfv473Jqhjh"
            + "kAPywbhgUbzSnC02wwZAyqBYOUzIUUoY4XRe6SVr/Q8lVsYbf4UBLkS2kBk1aOIPxbOIaPVQtEQ8vUnZ/WlrSxTA+JCTNHMc4Ig+Ele"
            + "s+Jod+iR3N/jDDf74wxu4e/5+DmtE9mUyhdgFNq7bZ3ekehbruC6aTxS/c1rom6Z698WrEfIYxcn4JGTftLA7tzCnJeD41IJVC+U07k"
            + "umUHw3E47Vqh+xnULeFisYLx064mV8UTZibWFMmX0p23wBUEsHCE0EGH3yAAAAlwEAAFBLAwQUAAgICAAAAAAAAAAAAAAAAAAAAAAAJ"
            + "wA5AHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5LmRlYnVnX3BrbEZCNQBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpa"
            + "WlpaWlpaWlpaWlpaWlpaWlpaWlpaWrWST0+DMBiHW6bOod/BGS94kKpo2Mwyox5x3pbgiXSAFtdR/nQu3IwHiZ9oX88CaeGu9tL0efq"
            + "+v8P7fmiGA1wgTgoIcECZQqe6vmYD6G4hAJOcB1E8NazTm+ELyzY4C3Q0z8MsRwF+j4JlQUPEEo5wjH0WB9hCNFqgpOCExZY5QnnEw7"
            + "ME+0v8GuaIs8wnKI7RigVrKkBzm0lh2OdjkeHllG28f066vK6SfEypF60S+vuYt4gjj2fYr/uPrSvRv356TepfJ9iWJRN0OaELQSZN3"
            + "FRPNbcP1PTSntMr0x0HzLZQjPYIEo3UaFeiISRKH0Mil+BE/dyT1m7tCBLwVO1MX4DK3bbuTlXuy8r71j5Aoho66udAoseOnrdVzx28"
            + "UFW6ROuO/lT6QKKyo79VU54emj9QSwcInsUTEDMBAAAFAwAAUEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAZAAYAc2ltcGxlbW9kZWw"
            + "vY29uc3RhbnRzLnBrbEZCAgBaWoACKS5QSwcIbS8JVwQAAAAEAAAAUEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAATADsAc2ltcGxlbW"
            + "9kZWwvdmVyc2lvbkZCNwBaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaMwpQSwcI0"
            + "Z5nVQIAAAACAAAAUEsBAgAAAAAICAAAAAAAAFzqQQQ0AAAANAAAABQAAAAAAAAAAAAAAAAAAAAAAHNpbXBsZW1vZGVsL2RhdGEucGts"
            + "UEsBAgAAFAAICAgAAAAAAE0EGH3yAAAAlwEAAB0AAAAAAAAAAAAAAAAAhAAAAHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5UEs"
            + "BAgAAFAAICAgAAAAAAJ7FExAzAQAABQMAACcAAAAAAAAAAAAAAAAAAgIAAHNpbXBsZW1vZGVsL2NvZGUvX190b3JjaF9fLnB5LmRlYn"
            + "VnX3BrbFBLAQIAAAAACAgAAAAAAABtLwlXBAAAAAQAAAAZAAAAAAAAAAAAAAAAAMMDAABzaW1wbGVtb2RlbC9jb25zdGFudHMucGtsU"
            + "EsBAgAAAAAICAAAAAAAANGeZ1UCAAAAAgAAABMAAAAAAAAAAAAAAAAAFAQAAHNpbXBsZW1vZGVsL3ZlcnNpb25QSwYGLAAAAAAAAAAe"
            + "Ay0AAAAAAAAAAAAFAAAAAAAAAAUAAAAAAAAAagEAAAAAAACSBAAAAAAAAFBLBgcAAAAA/AUAAAAAAAABAAAAUEsFBgAAAAAFAAUAagE"
            + "AAJIEAAAAAA==";
    static final long RAW_MODEL_SIZE; // size of the model before base64 encoding
    static {
        RAW_MODEL_SIZE = Base64.getDecoder().decode(BASE_64_ENCODED_MODEL).length;
    }

    protected void createBasicModel(String modelId) throws IOException {
        createPassThroughModel(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        putModelDefinition(modelId, BASE_64_ENCODED_MODEL, RAW_MODEL_SIZE);
    }

    private void putModelDefinition(String modelId, String base64EncodedModel, long unencodedModelSize) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        String body = Strings.format("""
            {"total_definition_length":%s,"definition": "%s","total_parts": 1}""", unencodedModelSize, base64EncodedModel);
        request.setJsonEntity(body);
        System.out.println("putModelDefiniton:" + client().performRequest(request));
    }

    private void createPassThroughModel(String modelId) throws IOException {
        createPassThroughModel(modelId, 0, 0);
    }

    private void createPassThroughModel(String modelId, long perDeploymentMemoryBytes, long perAllocationMemoryBytes) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        String metadata;
        if (perDeploymentMemoryBytes > 0 && perAllocationMemoryBytes > 0) {
            metadata = org.elasticsearch.core.Strings.format("""
                "metadata": {
                  "per_deployment_memory_bytes": %d,
                  "per_allocation_memory_bytes": %d
                },""", perDeploymentMemoryBytes, perAllocationMemoryBytes);
        } else {
            metadata = "";
        }
        request.setJsonEntity(org.elasticsearch.core.Strings.format("""
            {
               "description": "simple model for testing",
               "model_type": "pytorch",
                %s
               "inference_config": {
                 "pass_through": {
                   "tokenization": {
                     "bert": {
                       "with_special_tokens": false
                     }
                   }
                 }
               }
             }""", metadata));
        System.out.println("createPassThroughModel:" + client().performRequest(request));
    }

    private void putVocabulary(List<String> vocabulary, String modelId) throws IOException {
        List<String> vocabularyWithPad = new ArrayList<>();
        // vocabularyWithPad.add(BertTokenizer.PAD_TOKEN);
        // vocabularyWithPad.add(BertTokenizer.UNKNOWN_TOKEN);
        vocabularyWithPad.addAll(vocabulary);
        String quotedWords = vocabularyWithPad.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));

        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/vocabulary");
        request.setJsonEntity(org.elasticsearch.core.Strings.format("""
            { "vocabulary": [%s] }
            """, quotedWords));
        System.out.println("putVocabulary:" + client().performRequest(request));
    }

    protected void startDeployment(String modelId) throws IOException {
        Request request = new Request("POST", "_ml/trained_models/" + modelId + "/deployment/_start?wait_for=started&timeout=1m");
        client().performRequest(request);
        System.out.println("startDeployment:" + client().performRequest(request));

    }

    protected Response deleteModel(String modelId, boolean force) throws IOException {
        Request request = new Request("DELETE", "/_ml/trained_models/" + modelId + "?force=" + force);
        return client().performRequest(request);
    }

    protected Map<String, Object> putModel(String modelId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(modelConfig);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> getModels(String modelId, TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> getAllModels() throws IOException {
        var endpoint = Strings.format("_inference/_all");
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> inferOnMockService(String modelId, TaskType taskType, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("POST", endpoint);

        var bodyBuilder = new StringBuilder("{\"input\": [");
        for (var in : input) {
            bodyBuilder.append('"').append(in).append('"').append(',');
        }
        // remove last comma
        bodyBuilder.deleteCharAt(bodyBuilder.length() - 1);
        bodyBuilder.append("]}");

        request.setJsonEntity(bodyBuilder.toString());
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    @SuppressWarnings("unchecked")
    protected void assertNonEmptyInferenceResults(Map<String, Object> resultMap, int expectedNumberOfResults, TaskType taskType) {
        if (taskType == TaskType.SPARSE_EMBEDDING) {
            var results = (List<Map<String, Object>>) resultMap.get(TaskType.SPARSE_EMBEDDING.toString());
            assertThat(results, hasSize(expectedNumberOfResults));
        } else {
            fail("test with task type [" + taskType + "] are not supported yet");
        }
    }

    protected static void assertOkOrCreated(Response response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        // Once EntityUtils.toString(entity) is called the entity cannot be reused.
        // Avoid that call with check here.
        if (statusCode == 200 || statusCode == 201) {
            return;
        }

        String responseStr = EntityUtils.toString(response.getEntity());
        assertThat(responseStr, response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }
}
