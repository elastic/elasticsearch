/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.client.WarningsHandler.PERMISSIVE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MlAssignmentPlannerUpgradeIT extends AbstractUpgradeTestCase {

    private static final boolean IS_SINGLE_PROCESSOR_TEST = Boolean.parseBoolean(
        System.getProperty("tests.configure_test_clusters_with_one_processor", "false")
    );

    private Logger logger = LogManager.getLogger(MlAssignmentPlannerUpgradeIT.class);

    // See PyTorchModelIT for how this model was created
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/101926")
    public void testMlAssignmentPlannerUpgrade() throws Exception {
        assumeFalse("This test deploys multiple models which cannot be accommodated on a single processor", IS_SINGLE_PROCESSOR_TEST);

        logger.info("Starting testMlAssignmentPlannerUpgrade, model size {}", RAW_MODEL_SIZE);

        switch (CLUSTER_TYPE) {
            case OLD -> {
                // setup deployments using old and new memory format
                setupDeployments();

                waitForDeploymentStarted("old_memory_format");
                waitForDeploymentStarted("new_memory_format");

                // assert correct memory format is used
                assertOldMemoryFormat("old_memory_format");
                assertNewMemoryFormat("new_memory_format");
            }
            case MIXED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                waitForDeploymentStarted("old_memory_format");
                waitForDeploymentStarted("new_memory_format");

                // assert correct memory format is used
                assertOldMemoryFormat("old_memory_format");
                assertNewMemoryFormat("new_memory_format");
            }
            case UPGRADED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                waitForDeploymentStarted("old_memory_format");
                waitForDeploymentStarted("new_memory_format");

                // assert correct memory format is used
                assertOldMemoryFormat("old_memory_format");
                assertNewMemoryFormat("new_memory_format");

                cleanupDeployments();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void waitForDeploymentStarted(String modelId) throws Exception {
        assertBusy(() -> {
            var response = getTrainedModelStats(modelId);
            Map<String, Object> map = entityAsMap(response);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
            assertThat(stats, hasSize(1));
            var stat = stats.get(0);
            assertThat(stat.toString(), XContentMapValues.extractValue("deployment_stats.state", stat), equalTo("started"));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void assertOldMemoryFormat(String modelId) throws Exception {
        var response = getTrainedModelStats(modelId);
        Map<String, Object> map = entityAsMap(response);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
        assertThat(stats, hasSize(1));
        var stat = stats.get(0);
        Long expectedMemoryUsage = ByteSizeValue.ofMb(240).getBytes() + RAW_MODEL_SIZE * 2;
        Integer actualMemoryUsage = (Integer) XContentMapValues.extractValue("model_size_stats.required_native_memory_bytes", stat);
        assertThat(
            Strings.format("Memory usage mismatch for the model %s in cluster state %s", modelId, CLUSTER_TYPE.toString()),
            actualMemoryUsage,
            equalTo(expectedMemoryUsage.intValue())
        );
    }

    @SuppressWarnings("unchecked")
    private void assertNewMemoryFormat(String modelId) throws Exception {
        var response = getTrainedModelStats(modelId);
        Map<String, Object> map = entityAsMap(response);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
        assertThat(stats, hasSize(1));
        var stat = stats.get(0);
        Long expectedMemoryUsage = ByteSizeValue.ofMb(300).getBytes() + RAW_MODEL_SIZE + ByteSizeValue.ofMb(10).getBytes();
        Integer actualMemoryUsage = (Integer) XContentMapValues.extractValue("model_size_stats.required_native_memory_bytes", stat);
        assertThat(stat.toString(), actualMemoryUsage.toString(), equalTo(expectedMemoryUsage.toString()));
    }

    private Response getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(PERMISSIVE).build());
        var response = client().performRequest(request);
        assertOK(response);
        return response;
    }

    private Response infer(String input, String modelId) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_infer");
        request.setJsonEntity(Strings.format("""
            {  "docs": [{"input":"%s"}] }
            """, input));
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(PERMISSIVE).build());
        var response = client().performRequest(request);
        assertOK(response);
        return response;
    }

    private void putModelDefinition(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        request.setJsonEntity(Strings.format("""
            {"total_definition_length":%s,"definition": "%s","total_parts": 1}""", RAW_MODEL_SIZE, BASE_64_ENCODED_MODEL));
        client().performRequest(request);
    }

    private void putVocabulary(List<String> vocabulary, String modelId) throws IOException {
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

    private void setupDeployments() throws Exception {
        createTrainedModel("old_memory_format", 0, 0);
        putModelDefinition("old_memory_format");
        putVocabulary(List.of("these", "are", "my", "words"), "old_memory_format");
        startDeployment("old_memory_format");

        createTrainedModel("new_memory_format", ByteSizeValue.ofMb(300).getBytes(), ByteSizeValue.ofMb(10).getBytes());
        putModelDefinition("new_memory_format");
        putVocabulary(List.of("these", "are", "my", "words"), "new_memory_format");
        startDeployment("new_memory_format");
    }

    private void cleanupDeployments() throws IOException {
        stopDeployment("old_memory_format");
        deleteTrainedModel("old_memory_format");
        stopDeployment("new_memory_format");
        deleteTrainedModel("new_memory_format");
    }

    private void createTrainedModel(String modelId, long perDeploymentMemoryBytes, long perAllocationMemoryBytes) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        if (perAllocationMemoryBytes > 0 && perDeploymentMemoryBytes > 0) {
            request.setJsonEntity(Strings.format("""
                {
                   "description": "simple model for testing",
                   "model_type": "pytorch",
                   "inference_config": {
                     "pass_through": {
                       "tokenization": {
                         "bert": {
                           "with_special_tokens": false
                         }
                       }
                     }
                   },
                   "metadata": {
                     "per_deployment_memory_bytes": %s,
                     "per_allocation_memory_bytes": %s
                   }
                 }""", perDeploymentMemoryBytes, perAllocationMemoryBytes));
        } else {
            request.setJsonEntity("""
                {
                   "description": "simple model for testing",
                   "model_type": "pytorch",
                   "inference_config": {
                     "pass_through": {
                       "tokenization": {
                         "bert": {
                           "with_special_tokens": false
                         }
                       }
                     }
                   }
                 }""");
        }
        client().performRequest(request);
    }

    private void deleteTrainedModel(String modelId) throws IOException {
        Request request = new Request("DELETE", "_ml/trained_models/" + modelId);
        client().performRequest(request);
    }

    private Response startDeployment(String modelId) throws IOException {
        return startDeployment(modelId, "started");
    }

    private Response startDeployment(String modelId, String waitForState) throws IOException {
        String inferenceThreadParamName = "threads_per_allocation";
        String modelThreadParamName = "number_of_allocations";
        String compatibleHeader = null;
        if (CLUSTER_TYPE.equals(ClusterType.OLD) || CLUSTER_TYPE.equals(ClusterType.MIXED)) {
            compatibleHeader = compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_8);
            inferenceThreadParamName = "inference_threads";
            modelThreadParamName = "model_threads";
        }

        Request request = new Request(
            "POST",
            "/_ml/trained_models/"
                + modelId
                + "/deployment/_start?timeout=40s&wait_for="
                + waitForState
                + "&"
                + inferenceThreadParamName
                + "=1&"
                + modelThreadParamName
                + "=1"
        );
        if (compatibleHeader != null) {
            request.setOptions(request.getOptions().toBuilder().addHeader("Accept", compatibleHeader).build());
        }
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(PERMISSIVE).build());
        var response = client().performRequest(request);
        assertOK(response);
        return response;
    }

    private void stopDeployment(String modelId) throws IOException {
        String endpoint = "/_ml/trained_models/" + modelId + "/deployment/_stop";
        Request request = new Request("POST", endpoint);
        client().performRequest(request);
    }
}
