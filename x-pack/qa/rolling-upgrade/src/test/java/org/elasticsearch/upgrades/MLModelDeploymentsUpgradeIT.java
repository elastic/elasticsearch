/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

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
import static org.hamcrest.Matchers.oneOf;

public class MLModelDeploymentsUpgradeIT extends AbstractUpgradeTestCase {

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

    @BeforeClass
    public static void maybeSkip() {
        assumeFalse("Skip ML tests on unsupported glibc versions", SKIP_ML_TESTS);
    }

    @Before
    public void setUpLogging() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.ml.inference": "TRACE",
                "logger.org.elasticsearch.xpack.ml.inference.assignments": "DEBUG",
                "logger.org.elasticsearch.xpack.ml.process": "DEBUG",
                "logger.org.elasticsearch.xpack.ml.action": "TRACE"
              }
            }
            """);
        client().performRequest(request);
    }

    @After
    public void removeLogging() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.ml.inference": "INFO",
                "logger.org.elasticsearch.xpack.ml.process": "INFO",
                "logger.org.elasticsearch.xpack.ml.action": "INFO"
              }
            }
            """);
        client().performRequest(request);
    }

    public void testTrainedModelDeployment() throws Exception {
        assumeTrue("NLP model deployments added in 8.0", UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_0_0));

        final String modelId = "upgrade-deployment-test";

        switch (CLUSTER_TYPE) {
            case OLD -> {
                setupDeployment(modelId);
                assertInfer(modelId);
            }
            case MIXED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                waitForDeploymentStarted(modelId);
                // attempt inference on new and old nodes multiple times
                for (int i = 0; i < 10; i++) {
                    assertInfer(modelId);
                }
            }
            case UPGRADED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));

                waitForDeploymentStarted(modelId);
                assertInfer(modelId);
                assertNewInfer(modelId);
                stopDeployment(modelId);
            }
            default -> throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testTrainedModelDeploymentStopOnMixedCluster() throws Exception {
        assumeTrue("NLP model deployments added in 8.0", UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_0_0));

        final String modelId = "upgrade-deployment-test-stop-mixed-cluster";

        switch (CLUSTER_TYPE) {
            case OLD -> {
                setupDeployment(modelId);
                assertInfer(modelId);
            }
            case MIXED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                stopDeployment(modelId);
            }
            case UPGRADED -> {
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                assertThatTrainedModelAssignmentMetadataIsEmpty(modelId);
            }
            default -> throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void setupDeployment(String modelId) throws IOException {
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);
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
        return startDeployment(modelId, "started");
    }

    private Response startDeployment(String modelId, String waitForState) throws IOException {
        Request request = new Request(
            "POST",
            "/_ml/trained_models/"
                + modelId
                + "/deployment/_start?timeout=40s&wait_for="
                + waitForState
                + "&inference_threads=1&model_threads=1"
        );
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

    private void assertThatTrainedModelAssignmentMetadataIsEmpty(String modelId) throws IOException {
        Request getTrainedModelAssignmentMetadataRequest = new Request(
            "GET",
            "_cluster/state?filter_path=metadata.trained_model_assignment." + modelId
        );
        Response getTrainedModelAssignmentMetadataResponse = client().performRequest(getTrainedModelAssignmentMetadataRequest);
        String responseBody = EntityUtils.toString(getTrainedModelAssignmentMetadataResponse.getEntity());
        assertThat(responseBody, oneOf("{}", "{\"metadata\":{\"trained_model_assignment\":{}}}"));

        // trained_model_allocation was renamed to trained_model_assignment
        // in v8.3. The renaming happens automatically and the old
        // metadata should be removed once all nodes are upgraded.
        // However, sometimes there aren't enough cluster state change
        // events in the upgraded cluster test for this to happen
        getTrainedModelAssignmentMetadataRequest = new Request("GET", "_cluster/state?filter_path=metadata.trained_model_allocation");
        getTrainedModelAssignmentMetadataResponse = client().performRequest(getTrainedModelAssignmentMetadataRequest);
        responseBody = EntityUtils.toString(getTrainedModelAssignmentMetadataResponse.getEntity());
        assertThat(responseBody, oneOf("{}", "{\"metadata\":{\"trained_model_allocation\":{}}}"));
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
