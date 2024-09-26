/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.client.WarningsHandler.PERMISSIVE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MLModelDeploymentFullClusterRestartIT extends AbstractXpackFullClusterRestartTestCase {

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

    public MLModelDeploymentFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Before
    public void setLogging() throws IOException {
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference.assignment" : "TRACE",
                    "logger.org.elasticsearch.xpack.ml.process.assignment.planning" : "TRACE",
                    "logger.org.elasticsearch.xpack.ml.inference.deployment" : "TRACE",
                    "logger.org.elasticsearch.xpack.ml.process.logging" : "TRACE"
                }}""");
        client().performRequest(loggingSettings);
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testDeploymentSurvivesRestart() throws Exception {
        @UpdateForV9 // condition will always be true from v8, can be removed
        var originalClusterSupportsNlpModels = oldClusterHasFeature(RestTestLegacyFeatures.ML_NLP_SUPPORTED);
        assumeTrue("NLP model deployments added in 8.0", originalClusterSupportsNlpModels);

        String modelId = "trained-model-full-cluster-restart";

        if (isRunningAgainstOldCluster()) {
            createTrainedModel(modelId);
            putModelDefinition(modelId);
            putVocabulary(List.of("these", "are", "my", "words"), modelId);
            startDeployment(modelId);
            assertInfer(modelId);
        } else {
            ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                request.addParameter("wait_for_status", "yellow");
                request.addParameter("timeout", "70s");
            }));
            waitForDeploymentStarted(modelId);
            assertBusy(() -> {
                try {
                    assertInfer(modelId);
                    assertNewInfer(modelId);
                } catch (ResponseException e) {
                    // assertBusy only loops on AssertionErrors, so we have
                    // to convert failure status exceptions to these
                    throw new AssertionError("Inference failed", e);
                }
            }, 90, TimeUnit.SECONDS);
            stopDeployment(modelId);
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
            assertThat(
                stat.toString(),
                XContentMapValues.extractValue("deployment_stats.allocation_status.state", stat),
                equalTo("fully_allocated")
            );
            assertThat(stat.toString(), XContentMapValues.extractValue("deployment_stats.state", stat), equalTo("started"));
        }, 90, TimeUnit.SECONDS);
    }

    private void assertInfer(String modelId) throws IOException {
        Response inference = infer("my words", modelId);
        assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"predicted_value\":[[1.0,1.0]]}"));
    }

    private void assertNewInfer(String modelId) throws IOException {
        Response inference = newInfer("my words", modelId);
        assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"inference_results\":[{\"predicted_value\":[[1.0,1.0]]}]}"));
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

    private void createTrainedModel(String modelId) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
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
        client().performRequest(request);
    }

    private Response startDeployment(String modelId) throws IOException {
        return startDeployment(modelId, AllocationStatus.State.STARTED.toString());
    }

    private Response startDeployment(String modelId, String waitForState) throws IOException {
        String inferenceThreadParamName = "threads_per_allocation";
        String modelThreadParamName = "number_of_allocations";
        String compatibleHeader = null;
        if (isRunningAgainstOldCluster()) {
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

    private Response getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
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

    private Response newInfer(String input, String modelId) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/_infer");
        request.setJsonEntity(Strings.format("""
            {  "docs": [{"input":"%s"}] }
            """, input));
        var response = client().performRequest(request);
        assertOK(response);
        return response;
    }
}
