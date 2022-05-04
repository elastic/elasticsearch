/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStatus;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.integration.InferenceIngestIT.putPipeline;
import static org.elasticsearch.xpack.ml.integration.InferenceIngestIT.simulateRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * This test uses a tiny hardcoded base64 encoded PyTorch TorchScript model.
 * The model was created with the following python script and returns a
 * Tensor of 1s. The simplicity of the model is not important as the aim
 * is to test loading a model into the PyTorch process and evaluating it.
 *
 * ## Start Python
 * import torch
 * class SuperSimple(torch.nn.Module):
 *     def forward(self, input_ids=None, token_type_ids=None, position_ids=None, inputs_embeds=None):
 *         return torch.ones((input_ids.size()[0], 2), dtype=torch.float32)
 *
 * model = SuperSimple()
 * input_ids = torch.tensor([1, 2, 3, 4, 5])
 * the_rest = torch.ones(5)
 * result = model.forward(input_ids, the_rest, the_rest, the_rest)
 * print(result)
 *
 * traced_model =  torch.jit.trace(model, (input_ids, the_rest, the_rest, the_rest))
 * torch.jit.save(traced_model, "simplemodel.pt")
 * ## End Python
 */
public class PyTorchModelIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

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

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @Before
    public void setLogging() throws IOException {
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference.allocation" : "TRACE",
                    "logger.org.elasticsearch.xpack.ml.inference.deployment" : "TRACE",
                    "logger.org.elasticsearch.xpack.ml.process.logging" : "TRACE"
                }}""");
        client().performRequest(loggingSettings);
    }

    @After
    public void cleanup() throws Exception {
        terminate(executorService);

        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference.allocation": null,
                    "logger.org.elasticsearch.xpack.ml.inference.deployment" : null,
                    "logger.org.elasticsearch.xpack.ml.process.logging" : null
                }}""");
        client().performRequest(loggingSettings);

        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        waitForPendingTasks(adminClient());
    }

    public void testEvaluate() throws IOException, InterruptedException {
        String modelId = "test_evaluate";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);
        CountDownLatch latch = new CountDownLatch(10);
        Queue<String> failures = new ConcurrentLinkedQueue<>();
        try {
            // Adding multiple inference calls to verify different calls get routed to separate nodes
            for (int i = 0; i < 10; i++) {
                executorService.execute(() -> {
                    try {
                        Response inference = infer("my words", modelId);
                        assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"predicted_value\":[[1.0,1.0]]}"));
                    } catch (IOException ex) {
                        failures.add(ex.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }
        } finally {
            assertTrue("timed-out waiting for inference requests after 30s", latch.await(30, TimeUnit.SECONDS));
            stopDeployment(modelId);
        }
        if (failures.isEmpty() == false) {
            fail("Inference calls failed with [" + failures.stream().reduce((s1, s2) -> s1 + ", " + s2) + "]");
        }
    }

    public void testEvaluateWithResultFieldOverride() throws IOException {
        String modelId = "test_evaluate";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);
        String resultsField = randomAlphaOfLength(10);
        Response inference = infer("my words", modelId, resultsField);
        assertThat(EntityUtils.toString(inference.getEntity()), equalTo("{\"" + resultsField + "\":[[1.0,1.0]]}"));
        stopDeployment(modelId);
    }

    public void testEvaluateWithMinimalTimeout() throws IOException {
        String modelId = "test_evaluate_timeout";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);
        // There is a race between inference and timeout so that
        // even with a zero timeout a valid inference response may
        // be returned.
        // The test asserts that if an error occurs it is a timeout error
        try {
            infer("my words", modelId, TimeValue.ZERO);
        } catch (ResponseException ex) {
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(408));
        }
        stopDeployment(modelId);
    }

    public void testDeleteFailureDueToDeployment() throws IOException {
        String modelId = "test_deployed_model_delete";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);
        Exception ex = expectThrows(Exception.class, () -> client().performRequest(new Request("DELETE", "_ml/trained_models/" + modelId)));
        assertThat(ex.getMessage(), containsString("Cannot delete model [test_deployed_model_delete] as it is currently deployed"));
        stopDeployment(modelId);
    }

    @SuppressWarnings("unchecked")
    public void testDeploymentStats() throws IOException {
        String model = "model_starting_test";
        String modelPartial = "model_partially_started";
        String modelStarted = "model_started";
        createTrainedModel(model);
        putVocabulary(List.of("once", "twice"), model);
        putModelDefinition(model);
        createTrainedModel(modelPartial);
        putVocabulary(List.of("once", "twice"), modelPartial);
        putModelDefinition(modelPartial);
        createTrainedModel(modelStarted);
        putVocabulary(List.of("once", "twice"), modelStarted);
        putModelDefinition(modelStarted);

        CheckedBiConsumer<String, AllocationStatus.State, IOException> assertAtLeast = (modelId, state) -> {
            startDeployment(modelId, state.toString());
            Response response = getTrainedModelStats(modelId);
            var responseMap = entityAsMap(response);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
            assertThat(stats, hasSize(1));
            String statusState = (String) XContentMapValues.extractValue("deployment_stats.allocation_status.state", stats.get(0));
            assertThat(responseMap.toString(), statusState, is(not(nullValue())));
            assertThat(AllocationStatus.State.fromString(statusState), greaterThanOrEqualTo(state));

            Integer byteSize = (Integer) XContentMapValues.extractValue("model_size_stats.model_size_bytes", stats.get(0));
            assertThat(responseMap.toString(), byteSize, is(not(nullValue())));
            assertThat(byteSize, equalTo((int) RAW_MODEL_SIZE));

            Integer requiredNativeMemory = (Integer) XContentMapValues.extractValue(
                "model_size_stats.required_native_memory_bytes",
                stats.get(0)
            );
            assertThat(responseMap.toString(), requiredNativeMemory, is(not(nullValue())));
            assertThat(requiredNativeMemory, equalTo((int) (ByteSizeValue.ofMb(240).getBytes() + 2 * RAW_MODEL_SIZE)));

            Response humanResponse = client().performRequest(new Request("GET", "/_ml/trained_models/" + modelId + "/_stats?human"));
            var humanResponseMap = entityAsMap(humanResponse);
            stats = (List<Map<String, Object>>) humanResponseMap.get("trained_model_stats");
            assertThat(stats, hasSize(1));
            String stringModelSizeBytes = (String) XContentMapValues.extractValue("model_size_stats.model_size", stats.get(0));
            assertThat(
                "stats response: " + responseMap + " human stats response" + humanResponseMap,
                stringModelSizeBytes,
                is(not(nullValue()))
            );
            assertThat(stringModelSizeBytes, equalTo("1.5kb"));
            String stringRequiredNativeMemory = (String) XContentMapValues.extractValue(
                "model_size_stats.required_native_memory",
                stats.get(0)
            );
            assertThat(
                "stats response: " + responseMap + " human stats response" + humanResponseMap,
                stringRequiredNativeMemory,
                is(not(nullValue()))
            );
            assertThat(stringRequiredNativeMemory, equalTo("240mb"));
            stopDeployment(modelId);
        };

        assertAtLeast.accept(model, AllocationStatus.State.STARTING);
        assertAtLeast.accept(modelPartial, AllocationStatus.State.STARTED);
        assertAtLeast.accept(modelStarted, AllocationStatus.State.FULLY_ALLOCATED);
    }

    @SuppressWarnings("unchecked")
    public void testLiveDeploymentStats() throws IOException {
        String modelA = "model_a";

        createTrainedModel(modelA);
        putVocabulary(List.of("once", "twice"), modelA);
        putModelDefinition(modelA);
        startDeployment(modelA, AllocationStatus.State.FULLY_ALLOCATED.toString());
        {
            Response noInferenceCallsStatsResponse = getTrainedModelStats(modelA);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) entityAsMap(noInferenceCallsStatsResponse).get(
                "trained_model_stats"
            );
            assertThat(stats, hasSize(1));

            List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "deployment_stats.nodes",
                stats.get(0)
            );
            int inferenceCount = sumInferenceCountOnNodes(nodes);
            assertThat(inferenceCount, equalTo(0));

            for (var node : nodes) {
                // null before the model is used
                assertThat(node.get("last_access"), nullValue());
                assertThat(node.get("average_inference_time_ms"), nullValue());
                assertThat(node.get("average_inference_time_ms_last_minute"), nullValue());
            }
        }

        infer("once", modelA);
        infer("twice", modelA);
        {
            Response postInferStatsResponse = getTrainedModelStats(modelA);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) entityAsMap(postInferStatsResponse).get("trained_model_stats");
            assertThat(stats, hasSize(1));
            assertThat(XContentMapValues.extractValue("deployment_stats.model_id", stats.get(0)), equalTo(modelA));
            assertThat(XContentMapValues.extractValue("model_size_stats.model_size_bytes", stats.get(0)), equalTo((int) RAW_MODEL_SIZE));
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "deployment_stats.nodes",
                stats.get(0)
            );
            // 2 of the 3 nodes in the cluster are ML nodes
            assertThat(nodes, hasSize(2));
            for (var node : nodes) {
                assertThat(node.get("number_of_pending_requests"), notNullValue());
            }
            // last_access and average_inference_time_ms may be null if inference wasn't performed on this node
            assertAtLeastOneOfTheseIsNotNull("last_access", nodes);
            assertAtLeastOneOfTheseIsNotNull("average_inference_time_ms", nodes);

            int inferenceCount = sumInferenceCountOnNodes(nodes);
            assertThat(inferenceCount, equalTo(2));
        }
    }

    private void assertAtLeastOneOfTheseIsNotNull(String name, List<Map<String, Object>> nodes) {
        assertTrue("all nodes have null value for [" + name + "]", nodes.stream().anyMatch(n -> n.get(name) != null));
    }

    @SuppressWarnings("unchecked")
    public void testGetDeploymentStats_WithWildcard() throws IOException {
        String modelFoo = "foo";
        createTrainedModel(modelFoo);
        putVocabulary(List.of("once", "twice"), modelFoo);
        putModelDefinition(modelFoo);

        String modelBar = "bar";
        createTrainedModel(modelBar);
        putVocabulary(List.of("once", "twice"), modelBar);
        putModelDefinition(modelBar);

        startDeployment(modelFoo, AllocationStatus.State.FULLY_ALLOCATED.toString());
        startDeployment(modelBar, AllocationStatus.State.FULLY_ALLOCATED.toString());
        infer("once", modelFoo);
        infer("once", modelBar);
        {
            Response response = getTrainedModelStats("f*");
            Map<String, Object> map = entityAsMap(response);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
            assertThat(stats, hasSize(1));
            assertThat(XContentMapValues.extractValue("deployment_stats.model_id", stats.get(0)), equalTo(modelFoo));
        }
        {
            Response response = getTrainedModelStats("bar");
            Map<String, Object> map = entityAsMap(response);
            List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
            assertThat(stats, hasSize(1));
            assertThat(XContentMapValues.extractValue("deployment_stats.model_id", stats.get(0)), equalTo(modelBar));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetDeploymentStats_WithStartedStoppedDeployments() throws IOException {
        String modelFoo = "foo";
        String modelBar = "foo-2";
        createTrainedModel(modelFoo);
        putVocabulary(List.of("once", "twice"), modelFoo);
        putModelDefinition(modelFoo);

        createTrainedModel(modelBar);
        putVocabulary(List.of("once", "twice"), modelBar);
        putModelDefinition(modelBar);

        startDeployment(modelFoo, AllocationStatus.State.FULLY_ALLOCATED.toString());
        startDeployment(modelBar, AllocationStatus.State.FULLY_ALLOCATED.toString());
        infer("once", modelFoo);
        infer("once", modelBar);

        Response response = getTrainedModelStats("foo*");
        Map<String, Object> map = entityAsMap(response);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) map.get("trained_model_stats");
        assertThat(stats, hasSize(2));

        // check all nodes are started
        for (int i : new int[] { 0, 1 }) {
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "deployment_stats.nodes",
                stats.get(i)
            );
            // 2 ml nodes
            assertThat(nodes, hasSize(2));
            for (int j : new int[] { 0, 1 }) {
                Object state = XContentMapValues.extractValue("routing_state.routing_state", nodes.get(j));
                assertEquals("started", state);
            }
        }

        stopDeployment(modelFoo);

        response = getTrainedModelStats("foo*");
        map = entityAsMap(response);
        stats = (List<Map<String, Object>>) map.get("trained_model_stats");

        assertThat(stats, hasSize(2));
        assertThat(stats.get(0), not(hasKey("deployment_stats")));

        // check all nodes are started for the non-stopped deployment
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "deployment_stats.nodes",
            stats.get(1)
        );
        // 2 ml nodes
        assertThat(nodes, hasSize(2));
        for (int j : new int[] { 0, 1 }) {
            Object state = XContentMapValues.extractValue("routing_state.routing_state", nodes.get(j));
            assertEquals("started", state);
        }

        stopDeployment(modelBar);
    }

    public void testInferWithMissingModel() {
        Exception ex = expectThrows(Exception.class, () -> infer("foo", "missing_model"));
        assertThat(ex.getMessage(), containsString("Could not find trained model [missing_model]"));
    }

    public void testGetPytorchModelWithDefinition() throws IOException {
        String model = "should-fail-get";
        createTrainedModel(model);
        putVocabulary(List.of("once", "twice"), model);
        putModelDefinition(model);
        Exception ex = expectThrows(
            Exception.class,
            () -> client().performRequest(new Request("GET", "_ml/trained_models/" + model + "?include=definition"))
        );
        assertThat(ex.getMessage(), containsString("[should-fail-get] is type [pytorch] and does not support retrieving the definition"));
    }

    public void testStartDeploymentWithTruncatedDefinition() throws IOException {
        String model = "should-fail-get";
        createTrainedModel(model);
        putVocabulary(List.of("once", "twice"), model);
        Request request = new Request("PUT", "_ml/trained_models/" + model + "/definition/0");
        request.setJsonEntity("""
            {"total_definition_length":%s2,"definition": "%s","total_parts": 1}""".formatted(RAW_MODEL_SIZE, BASE_64_ENCODED_MODEL));
        client().performRequest(request);
        Exception ex = expectThrows(Exception.class, () -> startDeployment(model));
        assertThat(
            ex.getMessage(),
            containsString("Model definition truncated. Unable to deserialize trained model definition [" + model + "]")
        );
    }

    public void testInferencePipelineAgainstUnallocatedModel() throws IOException {
        String model = "not-deployed";
        createTrainedModel(model);
        putVocabulary(List.of("once", "twice"), model);
        putModelDefinition(model);

        String source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "model_id": "not-deployed"
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "input": "my words"
                  }
                }
              ]
            }""";

        String response = EntityUtils.toString(client().performRequest(simulateRequest(source)).getEntity());
        assertThat(
            response,
            allOf(
                containsString("model [not-deployed] must be deployed to use. Please deploy with the start trained model deployment API."),
                containsString("error"),
                not(containsString("warning"))
            )
        );

        client().performRequest(putPipeline("my_pipeline", """
            {"processors": [
                  {
                    "inference": {
                      "model_id": "not-deployed"
                    }
                  }
                ]
            }"""));

        Request request = new Request("PUT", "undeployed_model_index/_doc/1?pipeline=my_pipeline&refresh=true");
        request.setJsonEntity("""
            {
                    "input": "my words"
                  }
            """);
        Exception ex = expectThrows(Exception.class, () -> client().performRequest(request));
        assertThat(ex.getMessage(), containsString("Trained model [not-deployed] is not deployed."));
    }

    public void testTruncation() throws IOException {
        String modelId = "no-truncation";

        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity("""
            {
                "description": "simple model for testing",
                "model_type": "pytorch",
                "inference_config": {
                    "pass_through": {
                        "tokenization": {
                            "bert": {
                                "with_special_tokens": false,
                                "truncate": "none",
                                "max_sequence_length": 2
                            }
                        }
                    }
                }
            }""");
        client().performRequest(request);

        putVocabulary(List.of("once", "twice", "thrice"), modelId);
        putModelDefinition(modelId);
        startDeployment(modelId, AllocationStatus.State.FULLY_ALLOCATED.toString());

        String input = "once twice thrice";
        var e = expectThrows(ResponseException.class, () -> EntityUtils.toString(infer("once twice thrice", modelId).getEntity()));
        assertThat(
            e.getMessage(),
            containsString("Input too large. The tokenized input length [3] exceeds the maximum sequence length [2]")
        );

        request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_infer");
        request.setJsonEntity("""
            {
              "docs": [
                {
                  "input": "%s"
                }
              ],
              "inference_config": {
                "pass_through": {
                  "tokenization": {
                    "bert": {
                      "truncate": "first"
                    }
                  }
                }
              }
            }""".formatted(input));
        client().performRequest(request);
    }

    public void testStopUsedDeploymentByIngestProcessor() throws IOException {
        String modelId = "test_stop_used_deployment_by_ingest_processor";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);

        client().performRequest(putPipeline("my_pipeline", """
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s"
                  }
                }
              ]
            }""".formatted(modelId)));
        ResponseException ex = expectThrows(ResponseException.class, () -> stopDeployment(modelId));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(409));
        assertThat(
            EntityUtils.toString(ex.getResponse().getEntity()),
            containsString(
                "Cannot stop deployment for model [test_stop_used_deployment_by_ingest_processor] as it is referenced by"
                    + " ingest processors; use force to stop the deployment"
            )
        );

        stopDeployment(modelId, true);
    }

    public void testPipelineWithBadProcessor() throws IOException {
        String model = "deployed";
        createTrainedModel(model);
        putVocabulary(List.of("once", "twice"), model);
        putModelDefinition(model);
        startDeployment(model);
        String source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "model_id": "deployed",
                      "inference_config": {
                        "ner": {}
                      }
                    }
                  }
                ]
              },
              "docs": [
                {"_source": {"input": "my words"}}]
            }
            """;

        String response = EntityUtils.toString(client().performRequest(simulateRequest(source)).getEntity());
        assertThat(
            response,
            allOf(
                containsString("Trained model [deployed] is configured for task [pass_through] but called with task [ner]"),
                containsString("error"),
                not(containsString("warning"))
            )
        );

        // Missing input field is a warning
        source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "model_id": "deployed"
                    }
                  }
                ]
              },
              "docs": [
                {"_source": {"something": "my words"}}]
            }
            """;

        response = EntityUtils.toString(client().performRequest(simulateRequest(source)).getEntity());
        assertThat(response, containsString("Input field [input] does not exist in the source document"));
        assertThat(response, containsString("status_exception"));
    }

    public void testDeleteModelWithDeploymentUsedByIngestProcessor() throws IOException {
        String modelId = "test_delete_model_with_used_deployment";
        createTrainedModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
        startDeployment(modelId);

        ResponseException ex = expectThrows(ResponseException.class, () -> deleteModel(modelId, false));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(409));
        assertThat(
            EntityUtils.toString(ex.getResponse().getEntity()),
            containsString(
                "Cannot delete model [test_delete_model_with_used_deployment] as it is currently deployed;"
                    + " use force to delete the model"
            )
        );

        deleteModel(modelId, true);

        assertThatTrainedModelAllocationMetadataIsEmpty();
    }

    private int sumInferenceCountOnNodes(List<Map<String, Object>> nodes) {
        int inferenceCount = 0;
        for (var node : nodes) {
            inferenceCount += (Integer) node.get("inference_count");
        }
        return inferenceCount;
    }

    private void putModelDefinition(String modelId) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        request.setJsonEntity("""
            {"total_definition_length":%s,"definition": "%s","total_parts": 1}""".formatted(RAW_MODEL_SIZE, BASE_64_ENCODED_MODEL));
        client().performRequest(request);
    }

    private void putVocabulary(List<String> vocabulary, String modelId) throws IOException {
        List<String> vocabularyWithPad = new ArrayList<>();
        vocabularyWithPad.add(BertTokenizer.PAD_TOKEN);
        vocabularyWithPad.add(BertTokenizer.UNKNOWN_TOKEN);
        vocabularyWithPad.addAll(vocabulary);
        String quotedWords = vocabularyWithPad.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));

        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/vocabulary");
        request.setJsonEntity("""
            { "vocabulary": [%s] }
            """.formatted(quotedWords));
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
        Request request = new Request(
            "POST",
            "/_ml/trained_models/"
                + modelId
                + "/deployment/_start?timeout=40s&wait_for="
                + waitForState
                + "&inference_threads=1&model_threads=1"
        );
        return client().performRequest(request);
    }

    private void stopDeployment(String modelId) throws IOException {
        stopDeployment(modelId, false);
    }

    private void stopDeployment(String modelId, boolean force) throws IOException {
        String endpoint = "/_ml/trained_models/" + modelId + "/deployment/_stop";
        if (force) {
            endpoint += "?force=true";
        }
        Request request = new Request("POST", endpoint);
        client().performRequest(request);
    }

    private Response getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
        return client().performRequest(request);
    }

    private Response infer(String input, String modelId, TimeValue timeout) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_infer?timeout=" + timeout.toString());
        request.setJsonEntity("""
            {  "docs": [{"input":"%s"}] }
            """.formatted(input));
        return client().performRequest(request);
    }

    private Response infer(String input, String modelId) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_infer");
        request.setJsonEntity("""
            {  "docs": [{"input":"%s"}] }
            """.formatted(input));
        return client().performRequest(request);
    }

    private Response infer(String input, String modelId, String resultsField) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_infer");
        request.setJsonEntity("""
            {
              "docs": [ { "input": "%s" } ],
              "inference_config": {
                "pass_through": {
                  "results_field": "%s"
                }
              }
            }""".formatted(input, resultsField));
        return client().performRequest(request);
    }

    private Response deleteModel(String modelId, boolean force) throws IOException {
        Request request = new Request("DELETE", "/_ml/trained_models/" + modelId + "?force=" + force);
        return client().performRequest(request);
    }

    private void assertThatTrainedModelAllocationMetadataIsEmpty() throws IOException {
        Request getTrainedModelAllocationMetadataRequest = new Request(
            "GET",
            "_cluster/state?filter_path=metadata.trained_model_allocation"
        );
        Response getTrainedModelAllocationMetadataResponse = client().performRequest(getTrainedModelAllocationMetadataRequest);
        assertThat(
            EntityUtils.toString(getTrainedModelAllocationMetadataResponse.getEntity()),
            containsString("\"trained_model_allocation\":{}")
        );
    }
}
