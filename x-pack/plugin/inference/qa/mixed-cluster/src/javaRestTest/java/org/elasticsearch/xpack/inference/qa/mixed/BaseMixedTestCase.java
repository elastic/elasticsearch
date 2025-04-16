/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BaseMixedTestCase extends MixedClusterSpecTestCase {
    protected static String getUrl(MockWebServer webServer) {
        return Strings.format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }

    @Override
    protected Settings restClientSettings() {
        String token = ESRestTestCase.basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected void delete(String inferenceId, TaskType taskType) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s/%s", taskType, inferenceId));
        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
    }

    protected void delete(String inferenceId) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s", inferenceId));
        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
    }

    protected Map<String, Object> getAll() throws IOException {
        var request = new Request("GET", "_inference/_all");
        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
        return ESRestTestCase.entityAsMap(response);
    }

    protected Map<String, Object> get(String inferenceId) throws IOException {
        var endpoint = Strings.format("_inference/%s", inferenceId);
        var request = new Request("GET", endpoint);
        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
        return ESRestTestCase.entityAsMap(response);
    }

    protected Map<String, Object> get(TaskType taskType, String inferenceId) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, inferenceId);
        var request = new Request("GET", endpoint);
        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
        return ESRestTestCase.entityAsMap(response);
    }

    protected Map<String, Object> inference(String inferenceId, TaskType taskType, String input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, inferenceId);
        var request = new Request("POST", endpoint);
        request.setJsonEntity("{\"input\": [" + '"' + input + '"' + "]}");

        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
        return ESRestTestCase.entityAsMap(response);
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

        var response = ESRestTestCase.client().performRequest(request);
        ESRestTestCase.assertOK(response);
        return ESRestTestCase.entityAsMap(response);
    }

    protected void put(String inferenceId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s?error_trace", taskType, inferenceId);
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(modelConfig);
        var response = ESRestTestCase.client().performRequest(request);
        logger.warn("PUT response: {}", response.toString());
        System.out.println("PUT response: " + response.toString());
        ESRestTestCase.assertOKAndConsume(response);
    }

    protected static void assertOkOrCreated(Response response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        // Once EntityUtils.toString(entity) is called the entity cannot be reused.
        // Avoid that call with check here.
        if (statusCode == 200 || statusCode == 201) {
            return;
        }

        String responseStr = EntityUtils.toString(response.getEntity());
        ESTestCase.assertThat(
            responseStr,
            response.getStatusLine().getStatusCode(),
            Matchers.anyOf(Matchers.equalTo(200), Matchers.equalTo(201))
        );
    }
}
