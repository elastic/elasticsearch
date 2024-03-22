/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class InferencePermissionsIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .user("test_inference_admin", PASSWORD, "inference_admin", false)
        .user("test_inference_user", PASSWORD, "inference_user", false)
        .user("test_no_privileged", PASSWORD, "", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // use the privileged users here but not in the tests
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testPermissions() throws IOException {
        var putRequest = new Request("PUT", "_inference/sparse_embedding/permissions_test");
        putRequest.setJsonEntity(InferenceBaseRestTest.mockSparseServiceModelConfig());
        var getAllRequest = new Request("GET", "_inference/sparse_embedding/_all");
        var deleteRequest = new Request("DELETE", "_inference/sparse_embedding/permissions_test");

        var putModelForTestingInference = new Request("PUT", "_inference/sparse_embedding/model_to_test_user_priv");
        putModelForTestingInference.setJsonEntity(InferenceBaseRestTest.mockSparseServiceModelConfig());

        var inferRequest = new Request("POST", "_inference/sparse_embedding/model_to_test_user_priv");
        var bodyBuilder = new StringBuilder("{\"input\": [");
        for (var in : new String[] { "foo", "bar" }) {
            bodyBuilder.append('"').append(in).append('"').append(',');
        }
        // remove last comma
        bodyBuilder.deleteCharAt(bodyBuilder.length() - 1);
        bodyBuilder.append("]}");
        inferRequest.setJsonEntity(bodyBuilder.toString());

        var deleteInferenceModel = new Request("DELETE", "_inference/sparse_embedding/model_to_test_user_priv");

        try (RestClient inferenceAdminClient = buildClient(inferenceAdminClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            makeRequest(inferenceAdminClient, putRequest, true);
            makeRequest(inferenceAdminClient, getAllRequest, true);
            makeRequest(inferenceAdminClient, deleteRequest, true);
            // create a model now as the other clients don't have the privilege to do so
            makeRequest(inferenceAdminClient, putModelForTestingInference, true);
            makeRequest(inferenceAdminClient, inferRequest, true);
        }

        try (RestClient inferenceUserClient = buildClient(inferenceUserClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            makeRequest(inferenceUserClient, putRequest, false);
            makeRequest(inferenceUserClient, getAllRequest, true);
            makeRequest(inferenceUserClient, inferRequest, true);
            makeRequest(inferenceUserClient, deleteInferenceModel, false);
        }

        try (RestClient unprivilegedClient = buildClient(unprivilegedUserClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            makeRequest(unprivilegedClient, putRequest, false);
            makeRequest(unprivilegedClient, getAllRequest, false);
            makeRequest(unprivilegedClient, inferRequest, false);
            makeRequest(unprivilegedClient, deleteInferenceModel, false);
        }
    }

    private Settings inferenceAdminClientSettings() {
        String token = basicAuthHeaderValue("test_inference_admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings inferenceUserClientSettings() {
        String token = basicAuthHeaderValue("test_inference_user", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings unprivilegedUserClientSettings() {
        String token = basicAuthHeaderValue("test_no_privileged", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /*
     * This makes the given request with the given client. It asserts a 200 response if expectSuccess is true, and asserts an exception
     * with a 403 response if expectStatus is false.
     */
    private void makeRequest(RestClient client, Request request, boolean expectSuccess) throws IOException {
        if (expectSuccess) {
            Response response = client.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        } else {
            ResponseException exception = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.FORBIDDEN.getStatus()));
        }
    }
}
