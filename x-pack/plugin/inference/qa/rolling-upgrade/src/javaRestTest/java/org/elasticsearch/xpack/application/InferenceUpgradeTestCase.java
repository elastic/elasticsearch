/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.upgrades.ParameterizedRollingUpgradeTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;

public class InferenceUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    static final String MODELS_RENAMED_TO_ENDPOINTS = "8.15.0";

    public InferenceUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    protected static String getUrl(MockWebServer webServer) {
        return format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }

    protected void delete(String inferenceId, TaskType taskType) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s/%s", taskType, inferenceId));
        var response = client().performRequest(request);
        assertOK(response);
    }

    protected void delete(String inferenceId) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s", inferenceId));
        var response = client().performRequest(request);
        assertOK(response);
    }

    protected Map<String, Object> getAll() throws IOException {
        var request = new Request("GET", "_inference/_all");
        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> get(String inferenceId) throws IOException {
        var endpoint = Strings.format("_inference/%s", inferenceId);
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> get(TaskType taskType, String inferenceId) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, inferenceId);
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Map<String, Object>> getMinimalConfigs() throws IOException {
        var endpoint = "_cluster/state?filter_path=metadata.model_registry";
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOK(response);
        return (Map<String, Map<String, Object>>) XContentMapValues.extractValue("metadata.model_registry.models", entityAsMap(response));
    }

    protected Map<String, Object> inference(String inferenceId, TaskType taskType, String input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, inferenceId);
        var request = new Request("POST", endpoint);
        request.setJsonEntity("{\"input\": [" + '"' + input + '"' + "]}");

        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> rerank(String inferenceId, List<String> inputs, String query) throws IOException {
        var endpoint = Strings.format("_inference/rerank/%s", inferenceId);
        var request = new Request("POST", endpoint);

        StringBuilder body = new StringBuilder("{").append("\"query\":\"").append(query).append("\",").append("\"input\":[");

        for (int i = 0; i < inputs.size(); i++) {
            body.append("\"").append(inputs.get(i)).append("\"");
            if (i < inputs.size() - 1) {
                body.append(",");
            }
        }

        body.append("]}");
        request.setJsonEntity(body.toString());

        var response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    protected void put(String inferenceId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s?error_trace", taskType, inferenceId);
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(modelConfig);
        var response = client().performRequest(request);
        assertOKAndConsume(response);
    }

    @SuppressWarnings("unchecked")
    protected void deleteAll() throws IOException {
        var endpoints = (List<Map<String, Object>>) get(TaskType.ANY, "*").get("endpoints");
        for (var endpoint : endpoints) {
            try {
                delete((String) endpoint.get("inference_id"));
            } catch (Exception exc) {
                assertThat(exc.getMessage(), containsString("reserved inference endpoint"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    // in version 8.15, there was a breaking change where "models" was renamed to "endpoints"
    LinkedList<Map<String, Object>> getConfigsWithBreakingChangeHandling(TaskType testTaskType, String oldClusterId) throws IOException {
        var response = get(testTaskType, oldClusterId);
        LinkedList<Map<String, Object>> configs;
        configs = new LinkedList<>((List<Map<String, Object>>) response.getOrDefault("endpoints", List.of()));
        configs.addAll((List<Map<String, Object>>) response.getOrDefault("models", List.of()));
        return configs;
    }
}
