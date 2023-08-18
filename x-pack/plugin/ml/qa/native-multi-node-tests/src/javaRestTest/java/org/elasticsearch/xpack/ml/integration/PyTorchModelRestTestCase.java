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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class PyTorchModelRestTestCase extends ESRestTestCase {

    protected static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    protected final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @Before
    public void setLogging() throws IOException {
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference.assignment" : "DEBUG",
                    "logger.org.elasticsearch.xpack.ml.inference.deployment" : "DEBUG",
                    "logger.org.elasticsearch.xpack.ml.inference.pytorch" : "DEBUG",
                    "logger.org.elasticsearch.xpack.ml.process.logging" : "DEBUG",
                    "logger.org.elasticsearch.xpack.ml.action" : "DEBUG"
                }}""");
        client().performRequest(loggingSettings);
    }

    @After
    public void cleanup() throws Exception {
        terminate(executorService);

        Request clusterSettings = new Request("PUT", "_cluster/settings");
        clusterSettings.setJsonEntity("""
            {"persistent" : {
                "logger.org.elasticsearch.xpack.ml.inference.assignment": null,
                "logger.org.elasticsearch.xpack.ml.inference.deployment" : null,
                "logger.org.elasticsearch.xpack.ml.inference.pytorch" : null,
                "logger.org.elasticsearch.xpack.ml.process.logging" : null,
                "cluster.routing.allocation.awareness.attributes": null,
                "xpack.ml.max_lazy_ml_nodes": null
            }}""");
        client().performRequest(clusterSettings);

        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        waitForPendingTasks(adminClient());
    }

    protected void assertOkWithErrorMessage(Response response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 201) {
            return;
        }

        String responseStr = EntityUtils.toString(response.getEntity());
        assertThat(responseStr, response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    protected void assertAtLeastOneOfTheseIsNotNull(String name, List<Map<String, Object>> nodes) {
        assertTrue("all nodes have null value for [" + name + "] in " + nodes, nodes.stream().anyMatch(n -> n.get(name) != null));
    }

    protected void assertAtLeastOneOfTheseIsNonZero(String name, List<Map<String, Object>> nodes) {
        assertTrue("all nodes have null or zero value for [" + name + "] in " + nodes, nodes.stream().anyMatch(n -> {
            Object o = n.get(name);
            if (o instanceof Number) {
                return ((Number) o).longValue() != 0;
            } else {
                return false;
            }
        }));
    }

    @SuppressWarnings("unchecked")
    protected void assertAllocationCount(String modelId, int expectedAllocationCount) throws IOException {
        Response response = getTrainedModelStats(modelId);
        var responseMap = entityAsMap(response);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
        assertThat(stats, hasSize(1));
        int allocations = (int) XContentMapValues.extractValue("deployment_stats.allocation_status.allocation_count", stats.get(0));
        assertThat(allocations, equalTo(expectedAllocationCount));
    }

    @SuppressWarnings("unchecked")
    protected void assertInferenceCountOnDeployment(int expectedCount, String deploymentId) throws IOException {
        Response statsResponse = getTrainedModelStats(deploymentId);
        Map<String, Object> stats = entityAsMap(statsResponse);
        List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");

        boolean deploymentFound = false;
        for (var statsMap : trainedModelStats) {
            var deploymentStats = (Map<String, Object>) XContentMapValues.extractValue("deployment_stats", statsMap);
            // find the matching deployment
            if (deploymentId.equals(deploymentStats.get("deployment_id"))) {
                List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue("nodes", deploymentStats);
                int inferenceCount = sumInferenceCountOnNodes(nodes);
                assertEquals(stats.toString(), expectedCount, inferenceCount);
                deploymentFound = true;
                break;
            }
        }

        assertTrue("No deployment stats found for deployment [" + deploymentId + "]", deploymentFound);
    }

    @SuppressWarnings("unchecked")
    protected void assertInferenceCountOnModel(int expectedCount, String modelId) throws IOException {
        Response statsResponse = getTrainedModelStats(modelId);
        Map<String, Object> stats = entityAsMap(statsResponse);
        List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");

        int summedCount = 0;
        for (var statsMap : trainedModelStats) {
            assertEquals(modelId, statsMap.get("model_id"));
            var deploymentStats = (Map<String, Object>) XContentMapValues.extractValue("deployment_stats", statsMap);
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) XContentMapValues.extractValue("nodes", deploymentStats);
            summedCount += sumInferenceCountOnNodes(nodes);
        }

        assertEquals(stats.toString(), expectedCount, summedCount);
    }

    protected int sumInferenceCountOnNodes(List<Map<String, Object>> nodes) {
        int inferenceCount = 0;
        for (var node : nodes) {
            inferenceCount += (Integer) node.get("inference_count");
        }
        return inferenceCount;
    }

    protected void putModelDefinition(String modelId, String base64EncodedModel, long unencodedModelSize) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/0");
        String body = Strings.format("""
            {"total_definition_length":%s,"definition": "%s","total_parts": 1}""", unencodedModelSize, base64EncodedModel);
        request.setJsonEntity(body);
        client().performRequest(request);
    }

    protected void putVocabulary(List<String> vocabulary, String modelId) throws IOException {
        List<String> vocabularyWithPad = new ArrayList<>();
        vocabularyWithPad.add(BertTokenizer.PAD_TOKEN);
        vocabularyWithPad.add(BertTokenizer.UNKNOWN_TOKEN);
        vocabularyWithPad.addAll(vocabulary);
        String quotedWords = vocabularyWithPad.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));

        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/vocabulary");
        request.setJsonEntity(Strings.format("""
            { "vocabulary": [%s] }
            """, quotedWords));
        client().performRequest(request);
    }

    protected void createPassThroughModel(String modelId) throws IOException {
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

    protected void createTextEmbeddingModel(String modelId) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity("""
            {
               "description": "a text embedding model",
               "model_type": "pytorch",
               "inference_config": {
                 "text_embedding": {
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

    protected Response startDeployment(String modelId) throws IOException {
        return startDeployment(modelId, AllocationStatus.State.STARTED);
    }

    protected Response startWithDeploymentId(String modelId, String deploymentId) throws IOException {
        return startDeployment(modelId, deploymentId, AllocationStatus.State.STARTED, 1, 1, Priority.NORMAL);
    }

    protected Response startDeployment(String modelId, AllocationStatus.State waitForState) throws IOException {
        return startDeployment(modelId, null, waitForState, 1, 1, Priority.NORMAL);
    }

    protected Response startDeployment(
        String modelId,
        String deploymentId,
        AllocationStatus.State waitForState,
        int numberOfAllocations,
        int threadsPerAllocation,
        Priority priority
    ) throws IOException {
        String endPoint = "/_ml/trained_models/"
            + modelId
            + "/deployment/_start?timeout=40s&wait_for="
            + waitForState
            + "&threads_per_allocation="
            + threadsPerAllocation
            + "&number_of_allocations="
            + numberOfAllocations
            + "&priority="
            + priority;

        if (deploymentId != null) {
            endPoint = endPoint + "&deployment_id=" + deploymentId;
        }

        Request request = new Request("POST", endPoint);
        return client().performRequest(request);
    }

    protected void stopDeployment(String modelId) throws IOException {
        stopDeployment(modelId, false);
    }

    protected void stopDeployment(String modelId, boolean force) throws IOException {
        String endpoint = "/_ml/trained_models/" + modelId + "/deployment/_stop";
        if (force) {
            endpoint += "?force=true";
        }
        Request request = new Request("POST", endpoint);
        client().performRequest(request);
    }

    protected Response updateDeployment(String modelId, int numberOfAllocations) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_update");
        request.setJsonEntity("{\"number_of_allocations\":" + numberOfAllocations + "}");
        return client().performRequest(request);
    }

    protected Response getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
        return client().performRequest(request);
    }

    protected Response infer(String input, String modelId, TimeValue timeout) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/_infer?timeout=" + timeout.toString());
        request.setJsonEntity(Strings.format("""
            {  "docs": [{"input":"%s"}] }
            """, input));
        return client().performRequest(request);
    }

    protected Response infer(String input, String modelId) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/_infer?timeout=30s");
        request.setJsonEntity(Strings.format("""
            {  "docs": [{"input":"%s"}] }
            """, input));
        return client().performRequest(request);
    }

    protected Response infer(String input, String modelId, String resultsField) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/_infer?timeout=30s");
        request.setJsonEntity(Strings.format("""
            {
              "docs": [ { "input": "%s" } ],
              "inference_config": {
                "pass_through": {
                  "results_field": "%s"
                }
              }
            }""", input, resultsField));
        return client().performRequest(request);
    }

    protected Response deleteModel(String modelId, boolean force) throws IOException {
        Request request = new Request("DELETE", "/_ml/trained_models/" + modelId + "?force=" + force);
        return client().performRequest(request);
    }

    protected void forceMergeIndex(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_forcemerge?max_num_segments=1");
        assertOkWithErrorMessage(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    protected int getAllocationCount(String modelId) throws IOException {
        Response response = getTrainedModelStats(modelId);
        var responseMap = entityAsMap(response);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) responseMap.get("trained_model_stats");
        assertThat(stats, hasSize(1));
        return (int) XContentMapValues.extractValue("deployment_stats.allocation_status.allocation_count", stats.get(0));
    }

    protected void assertThatTrainedModelAssignmentMetadataIsEmpty() throws IOException {
        Request getTrainedModelAssignmentMetadataRequest = new Request(
            "GET",
            "_cluster/state?filter_path=metadata.trained_model_assignment"
        );
        Response getTrainedModelAssignmentMetadataResponse = client().performRequest(getTrainedModelAssignmentMetadataRequest);
        assertThat(
            EntityUtils.toString(getTrainedModelAssignmentMetadataResponse.getEntity()),
            containsString("\"trained_model_assignment\":{}")
        );

        getTrainedModelAssignmentMetadataRequest = new Request("GET", "_cluster/state?filter_path=metadata.trained_model_allocation");
        getTrainedModelAssignmentMetadataResponse = client().performRequest(getTrainedModelAssignmentMetadataRequest);
        assertThat(EntityUtils.toString(getTrainedModelAssignmentMetadataResponse.getEntity()), equalTo("{}"));
    }

    protected void assertNotificationsContain(String modelId, String... auditMessages) throws IOException {
        client().performRequest(new Request("POST", ".ml-notifications-*/_refresh"));
        Request search = new Request("POST", ".ml-notifications-*/_search");
        search.setJsonEntity(Strings.format("""
            {
                "size": 100,
                "query": {
                  "bool": {
                    "filter": [
                      {"term": {"job_id": "%s"}},
                      {"term": {"job_type": "inference"}}
                    ]
                  }
                }
            }
            """, modelId));
        String response = EntityUtils.toString(client().performRequest(search).getEntity());
        for (String msg : auditMessages) {
            assertThat(response, containsString(msg));
        }
    }

    protected void assertSystemNotificationsContain(String... auditMessages) throws IOException {
        client().performRequest(new Request("POST", ".ml-notifications-*/_refresh"));
        Request search = new Request("POST", ".ml-notifications-*/_search");
        search.setJsonEntity("""
            {
                "size": 100,
                "query": {
                  "bool": {
                    "filter": [
                      {"term": {"job_type": "system"}}
                    ]
                  }
                }
            }
            """);
        String response = EntityUtils.toString(client().performRequest(search).getEntity());
        for (String msg : auditMessages) {
            assertThat(response, containsString(msg));
        }
    }
}
